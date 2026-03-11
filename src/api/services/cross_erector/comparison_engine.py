"""Cross-erector comparison engine — semantic grouping of exclusions across erectors."""

import logging
import time

from scope_classification import (
    ScopeAnalysisEngine,
    ClaudeService,
    ConnectionFactory,
)

from ._dataclasses.comparison_result import (
    ComparisonSessionResult,
    UnifiedItemResult,
    ErectorCoverage,
)
from ._errors.comparison_error import CrossErectorComparisonError

log = logging.getLogger(__name__)

MIN_ERECTORS = 2
MAX_ERECTORS = 15

GROUPING_SYSTEM_PROMPT = """You are analyzing steel erection scope letter exclusions for a structural steel fabrication company.

You will receive exclusion items extracted from multiple erectors' scope letters for the same job.
Your task is to semantically group identical or equivalent items across erectors under a single canonical description.

Rules:
- Two items are "equivalent" if they refer to the same scope element, even if worded differently.
  Example: "Fire watch" and "Fire watch not included in our scope" and "Cost of fire watch personnel" are all the same item.
- Each canonical item should have a short, clear description (5-15 words).
- For each canonical item, indicate which erectors mention it and whether it's an exclusion or inclusion.
- If an erector does not mention an item at all, mark them as "NotMentioned".
- Group items within the provided category. Do not re-categorize.

Respond ONLY with a JSON array. No preamble, no markdown fences."""

GROUPING_USER_PROMPT_TEMPLATE = """Category: {category_name}

Erectors and their items:

{erector_sections}

Return a JSON array of unified items:
[
  {{
    "canonical": "Short clear description",
    "erectors": {{
      "ErectorName": {{ "type": "Excludes", "source_ids": [123] }},
      "OtherErector": {{ "type": "NotMentioned", "source_ids": [] }}
    }}
  }}
]

Every erector MUST appear in every item's "erectors" object, even if "NotMentioned".
"type" must be one of: "Excludes", "Includes", "NotMentioned".
"source_ids" is the list of extraction IDs that map to this canonical item (can be multiple if compound)."""


class CrossErectorComparisonEngine:
    """Orchestrates cross-erector comparison: extract per-erector → group across erectors."""

    def __init__(
            self,
            db:      ConnectionFactory,
            claude:  ClaudeService,
            engine:  ScopeAnalysisEngine ) -> None:
        """Initialize with shared DB, Claude service, and analysis engine."""

        self._db     = db
        self._claude = claude
        self._engine = engine


    def update_phase(self, comparison_id: int, phase: str, erectors_analyzed: int | None = None) -> None:
        """Update the current phase and optionally erectors-analyzed count."""

        if erectors_analyzed is not None:
            self._db.execute(
                f"""
                UPDATE {self._db.schema}.ComparisonSessions
                SET CurrentPhase = ?, ErectorsAnalyzed = ?
                WHERE Id = ?
                """,
                (phase, erectors_analyzed, comparison_id),
            )
        else:
            self._db.execute(
                f"""
                UPDATE {self._db.schema}.ComparisonSessions
                SET CurrentPhase = ?
                WHERE Id = ?
                """,
                (phase, comparison_id),
            )

        self._db.commit()


    def create_comparison(
            self,
            analysis_session_ids: list[int],
            job_number:           str | None = None,
            job_name:             str | None = None,
            initiated_by:         str | None = None ) -> int:
        """Create a ComparisonSession from existing AnalysisSessions. Returns comparison_session_id."""

        if len(analysis_session_ids) < MIN_ERECTORS:
            raise CrossErectorComparisonError(
                f"Need at least {MIN_ERECTORS} erectors, got {len(analysis_session_ids)}",
                phase="validate",
            )

        if len(analysis_session_ids) > MAX_ERECTORS:
            raise CrossErectorComparisonError(
                f"Maximum {MAX_ERECTORS} erectors, got {len(analysis_session_ids)}",
                phase="validate",
            )

        # Validate all sessions exist and are complete
        erectors = self._validate_sessions(analysis_session_ids)

        # Create ComparisonSession
        cursor = self._db.execute(
            f"""
            INSERT INTO {self._db.schema}.ComparisonSessions
                (JobNumber, JobName, InitiatedBy, Status, TotalErectors)
            OUTPUT INSERTED.Id
            VALUES (?, ?, ?, 'Running', ?)
            """,
            (job_number, job_name, initiated_by, len(analysis_session_ids)),
        )
        row            = cursor.fetchone()
        comparison_id  = row[0]
        self._db.commit()

        # Link erector sessions
        for i, (session_id, erector_name) in enumerate(erectors):
            self._db.execute(
                f"""
                INSERT INTO {self._db.schema}.ComparisonSessionErectors
                    (ComparisonSessionId, AnalysisSessionId, ErectorNameRaw, SortOrder)
                VALUES (?, ?, ?, ?)
                """,
                (comparison_id, session_id, erector_name, i),
            )

        self._db.commit()
        log.info(f"Created ComparisonSession {comparison_id} with {len(erectors)} erectors")

        return comparison_id


    def run_grouping(self, comparison_id: int) -> ComparisonSessionResult:
        """Run the semantic grouping pipeline on a ComparisonSession."""

        t0 = time.perf_counter()

        try:
            # Load erector sessions
            erector_sessions = self._get_erector_sessions(comparison_id)

            if not erector_sessions:
                raise CrossErectorComparisonError(
                    "No erector sessions linked to this comparison",
                    phase="load", comparison_id=comparison_id,
                )

            erector_names = {s["AnalysisSessionId"]: s["ErectorNameRaw"] for s in erector_sessions}

            # Gather extracted exclusions grouped by category
            category_groups = self._gather_exclusions_by_category(
                [s["AnalysisSessionId"] for s in erector_sessions],
            )

            if not category_groups:
                raise CrossErectorComparisonError(
                    "No extracted exclusions found across linked sessions",
                    phase="gather", comparison_id=comparison_id,
                )

            # Clear any existing unified items (for re-runs / add-erector)
            self._clear_unified_items(comparison_id)

            self.update_phase(comparison_id, "Grouping")

            # Run AI grouping per category
            all_unified = []

            for category_id, category_name, items_by_erector in category_groups:
                unified = self._group_category(
                    comparison_id  = comparison_id,
                    category_id    = category_id,
                    category_name  = category_name,
                    items_by_erector = items_by_erector,
                    erector_names  = erector_names,
                )
                all_unified.extend(unified)

            # Update session counts + status
            elapsed_ms = int((time.perf_counter() - t0) * 1000)

            self._db.execute(
                f"""
                UPDATE {self._db.schema}.ComparisonSessions
                SET Status        = 'Complete',
                    CurrentPhase  = 'Complete',
                    TotalUnified  = ?,
                    TotalErectors = ?,
                    CompletedAt   = SYSUTCDATETIME()
                WHERE Id = ?
                """,
                (len(all_unified), len(erector_sessions), comparison_id),
            )
            self._db.commit()

            # Validation: count source items that made it into unified coverage
            total_input = sum(
                len(items)
                for _, _, items_by_session in category_groups
                for items in items_by_session.values()
            )

            cursor = self._db.execute(
                f"""
                SELECT COUNT(DISTINCT uc.ExtractedExclusionId)
                FROM {self._db.schema}.UnifiedItemCoverage uc
                JOIN {self._db.schema}.UnifiedItems ui ON ui.Id = uc.UnifiedItemId
                WHERE ui.ComparisonSessionId = ?
                  AND uc.ExtractedExclusionId IS NOT NULL
                """,
                (comparison_id,),
            )
            mapped_count = cursor.fetchone()[0]

            if mapped_count < total_input:
                dropped = total_input - mapped_count
                log.warning(
                    f"  ComparisonSession {comparison_id}: {dropped} of {total_input} "
                    f"extracted items were not mapped to any unified item"
                )

            log.info(
                f"ComparisonSession {comparison_id} complete: "
                f"{len(all_unified)} unified items across {len(erector_sessions)} erectors "
                f"({mapped_count}/{total_input} source items mapped, {elapsed_ms}ms)"
            )

            return ComparisonSessionResult(
                comparison_session_id = comparison_id,
                status                = "Complete",
                total_erectors        = len(erector_sessions),
                total_unified         = len(all_unified),
                unified_items         = all_unified,
                processing_time_ms    = elapsed_ms,
            )

        except CrossErectorComparisonError:
            raise

        except Exception as exc:
            self._db.execute(
                f"""
                UPDATE {self._db.schema}.ComparisonSessions
                SET Status = 'Error', CurrentPhase = 'Error', ErrorMessage = ?
                WHERE Id = ?
                """,
                (str(exc)[:2000], comparison_id),
            )
            self._db.commit()
            log.error(f"ComparisonSession {comparison_id} FAILED: {exc}")

            raise CrossErectorComparisonError(
                str(exc), phase="grouping", comparison_id=comparison_id,
            ) from exc


    def add_erector(
            self,
            comparison_id:      int,
            analysis_session_id: int ) -> None:
        """Add an erector to an existing comparison. Does NOT re-run grouping — caller does that."""

        # Validate session
        erectors = self._validate_sessions([analysis_session_id])
        session_id, erector_name = erectors[0]

        # Check not already linked
        cursor = self._db.execute(
            f"""
            SELECT Id FROM {self._db.schema}.ComparisonSessionErectors
            WHERE ComparisonSessionId = ? AND AnalysisSessionId = ?
            """,
            (comparison_id, analysis_session_id),
        )

        if cursor.fetchone():
            raise CrossErectorComparisonError(
                f"Session {analysis_session_id} already in comparison {comparison_id}",
                phase="add",
            )

        # Get current max sort order
        cursor = self._db.execute(
            f"""
            SELECT ISNULL(MAX(SortOrder), -1) + 1
            FROM {self._db.schema}.ComparisonSessionErectors
            WHERE ComparisonSessionId = ?
            """,
            (comparison_id,),
        )
        next_sort = cursor.fetchone()[0]

        # Check total erector count
        cursor = self._db.execute(
            f"""
            SELECT COUNT(*)
            FROM {self._db.schema}.ComparisonSessionErectors
            WHERE ComparisonSessionId = ?
            """,
            (comparison_id,),
        )
        current_count = cursor.fetchone()[0]

        if current_count >= MAX_ERECTORS:
            raise CrossErectorComparisonError(
                f"Comparison already has {current_count} erectors (max {MAX_ERECTORS})",
                phase="add",
            )

        # Insert link
        self._db.execute(
            f"""
            INSERT INTO {self._db.schema}.ComparisonSessionErectors
                (ComparisonSessionId, AnalysisSessionId, ErectorNameRaw, SortOrder)
            VALUES (?, ?, ?, ?)
            """,
            (comparison_id, session_id, erector_name, next_sort),
        )

        # Mark comparison as needing re-run
        self._db.execute(
            f"""
            UPDATE {self._db.schema}.ComparisonSessions
            SET Status = 'Running', TotalErectors = ? + 1
            WHERE Id = ?
            """,
            (current_count, comparison_id),
        )
        self._db.commit()

        log.info(f"Added session {session_id} ({erector_name}) to comparison {comparison_id}")


    def get_result(self, comparison_id: int) -> dict:
        """Full comparison result with erectors + unified items + coverage matrix."""

        # Comparison session header
        cursor = self._db.execute(
            f"SELECT * FROM {self._db.schema}.ComparisonSessions WHERE Id = ?",
            (comparison_id,),
        )
        columns = [col[0] for col in cursor.description]
        row     = cursor.fetchone()

        if not row:
            return {}

        session = dict(zip(columns, row))

        # Linked erectors
        cursor = self._db.execute(
            f"""
            SELECT cse.AnalysisSessionId, cse.ErectorNameRaw, cse.SortOrder,
                   s.JobNumber, s.SourceFileName
            FROM {self._db.schema}.ComparisonSessionErectors cse
            JOIN {self._db.schema}.AnalysisSessions s ON s.Id = cse.AnalysisSessionId
            WHERE cse.ComparisonSessionId = ?
            ORDER BY cse.SortOrder
            """,
            (comparison_id,),
        )
        columns  = [col[0] for col in cursor.description]
        erectors = [dict(zip(columns, r)) for r in cursor.fetchall()]

        # Unified items + coverage
        cursor = self._db.execute(
            f"""
            SELECT ui.Id AS UnifiedItemId,
                   ui.CanonicalDescription,
                   ui.CategoryId,
                   sc.Name AS CategoryName,
                   ui.SortOrder,
                   uc.AnalysisSessionId,
                   uc.ExtractedExclusionId,
                   uc.CoverageType,
                   uc.RawText
            FROM {self._db.schema}.UnifiedItems ui
            LEFT JOIN {self._db.schema}.ScopeCategories sc ON sc.Id = ui.CategoryId
            LEFT JOIN {self._db.schema}.UnifiedItemCoverage uc ON uc.UnifiedItemId = ui.Id
            WHERE ui.ComparisonSessionId = ?
            ORDER BY ui.SortOrder, ui.Id, uc.AnalysisSessionId
            """,
            (comparison_id,),
        )
        columns  = [col[0] for col in cursor.description]
        cov_rows = [dict(zip(columns, r)) for r in cursor.fetchall()]

        # Assemble into nested structure
        items_map: dict[int, dict] = {}

        for row in cov_rows:
            uid = row["UnifiedItemId"]

            if uid not in items_map:
                items_map[uid] = {
                    "id":          uid,
                    "description": row["CanonicalDescription"],
                    "category_id": row["CategoryId"],
                    "category":    row["CategoryName"],
                    "sort_order":  row["SortOrder"],
                    "coverage":    {},
                }

            if row["AnalysisSessionId"] is not None:
                items_map[uid]["coverage"][str(row["AnalysisSessionId"])] = {
                    "type":                  row["CoverageType"],
                    "raw":                   row["RawText"],
                    "extracted_exclusion_id": row["ExtractedExclusionId"],
                }

        unified_items = sorted(items_map.values(), key=lambda x: x["sort_order"])

        return {
            "comparison_session": session,
            "erectors":          erectors,
            "unified_items":     unified_items,
        }


    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_sessions(self, session_ids: list[int]) -> list[tuple[int, str]]:
        """Validate sessions exist and are Complete. Returns [(session_id, erector_name)]."""

        results = []

        for sid in session_ids:
            cursor = self._db.execute(
                f"""
                SELECT Id, Status, ErectorNameRaw
                FROM {self._db.schema}.AnalysisSessions
                WHERE Id = ? AND IsDeleted = 0
                """,
                (sid,),
            )
            row = cursor.fetchone()

            if not row:
                raise CrossErectorComparisonError(
                    f"AnalysisSession {sid} not found",
                    phase="validate",
                )

            session_id, status, erector_name = row

            if status not in ("Complete", "Classified"):
                raise CrossErectorComparisonError(
                    f"AnalysisSession {sid} is '{status}', expected 'Complete' or 'Classified'",
                    phase="validate",
                )

            results.append((session_id, erector_name or f"Erector_{sid}"))

        return results


    def _get_erector_sessions(self, comparison_id: int) -> list[dict]:
        """Get all linked erector sessions for a comparison."""

        cursor = self._db.execute(
            f"""
            SELECT AnalysisSessionId, ErectorNameRaw, SortOrder
            FROM {self._db.schema}.ComparisonSessionErectors
            WHERE ComparisonSessionId = ?
            ORDER BY SortOrder
            """,
            (comparison_id,),
        )
        columns = [col[0] for col in cursor.description]

        return [dict(zip(columns, r)) for r in cursor.fetchall()]


    def _gather_exclusions_by_category(
            self,
            session_ids: list[int] ) -> list[tuple[int, str, dict[int, list[dict]]]]:
        """Gather extracted exclusions grouped by category, then by session.

        Returns: [(category_id, category_name, {session_id: [items]})]
        """

        placeholders = ", ".join("?" for _ in session_ids)

        cursor = self._db.execute(
            f"""
            SELECT ee.Id, ee.SessionId, ee.RawText, ee.NormalizedText,
                   ee.CategoryId, sc.Name AS CategoryName
            FROM {self._db.schema}.ExtractedExclusions ee
            LEFT JOIN {self._db.schema}.ScopeCategories sc ON sc.Id = ee.CategoryId
            WHERE ee.SessionId IN ({placeholders})
            ORDER BY ISNULL(ee.CategoryId, 999999), ee.SessionId, ee.Id
            """,
            tuple(session_ids),
        )
        columns = [col[0] for col in cursor.description]
        rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]

        # Group by category → session
        categories: dict[int, tuple[str, dict[int, list[dict]]]] = {}

        for row in rows:
            cat_id   = row["CategoryId"] or -1
            cat_name = row["CategoryName"] or "Uncategorized"

            if cat_id not in categories:
                categories[cat_id] = (cat_name, {})

            session_id = row["SessionId"]

            if session_id not in categories[cat_id][1]:
                categories[cat_id][1][session_id] = []

            categories[cat_id][1][session_id].append(row)

        # Warn about uncategorized items
        if -1 in categories:
            uncat_count = sum(len(items) for items in categories[-1][1].values())
            log.warning(
                f"  {uncat_count} extracted exclusion(s) have no category — "
                f"included as 'Uncategorized'"
            )

        return [
            (cat_id, cat_name, items_by_session)
            for cat_id, (cat_name, items_by_session) in sorted(categories.items())
        ]


    def _group_category(
            self,
            comparison_id:    int,
            category_id:      int,
            category_name:    str,
            items_by_erector: dict[int, list[dict]],
            erector_names:    dict[int, str] ) -> list[UnifiedItemResult]:
        """Run AI semantic grouping for one category. Persists results and returns them."""

        # Build the prompt sections — one block per erector
        erector_sections = []

        for session_id, items in items_by_erector.items():
            name  = erector_names.get(session_id, f"Erector_{session_id}")
            lines = []

            for item in items:
                text = item.get("NormalizedText") or item["RawText"]
                lines.append(f"  - [ID:{item['Id']}] {text}")

            erector_sections.append(f"{name} (session {session_id}):\n" + "\n".join(lines))

        user_prompt = GROUPING_USER_PROMPT_TEMPLATE.format(
            category_name    = category_name,
            erector_sections = "\n\n".join(erector_sections),
        )

        # Call Claude
        try:
            grouped = self._claude.complete_json(
                system_prompt = GROUPING_SYSTEM_PROMPT,
                user_prompt   = user_prompt,
                max_tokens    = 8192,
                temperature   = 0.0,
            )
        except Exception as exc:
            log.error(f"  Grouping failed for category {category_name}: {exc}")

            raise CrossErectorComparisonError(
                f"AI grouping failed for {category_name}: {exc}",
                phase="grouping", comparison_id=comparison_id,
            ) from exc

        # Persist unified items + coverage
        results = []

        for sort_idx, item in enumerate(grouped):
            canonical = item.get("canonical", "Unknown item")

            # Insert UnifiedItem (NULL for uncategorized sentinel)
            db_category_id = category_id if category_id != -1 else None

            cursor = self._db.execute(
                f"""
                INSERT INTO {self._db.schema}.UnifiedItems
                    (ComparisonSessionId, CanonicalDescription, CategoryId, SortOrder)
                OUTPUT INSERTED.Id
                VALUES (?, ?, ?, ?)
                """,
                (comparison_id, canonical[:500], db_category_id, sort_idx),
            )
            unified_id = cursor.fetchone()[0]

            # Build coverage for all erectors
            erector_data = item.get("erectors", {})
            coverages    = []

            for session_id, name in erector_names.items():
                match = erector_data.get(name, {})
                cov_type   = match.get("type", "NotMentioned")
                source_ids = match.get("source_ids", [])

                # Validate coverage type
                if cov_type not in ("Excludes", "Includes", "NotMentioned"):
                    cov_type = "NotMentioned"

                # Find raw text from the first source ID
                raw_text       = None
                first_ext_id   = source_ids[0] if source_ids else None

                if first_ext_id and session_id in items_by_erector:
                    for ex_item in items_by_erector[session_id]:
                        if ex_item["Id"] == first_ext_id:
                            raw_text = ex_item.get("NormalizedText") or ex_item["RawText"]
                            break

                # Insert coverage row
                self._db.execute(
                    f"""
                    INSERT INTO {self._db.schema}.UnifiedItemCoverage
                        (UnifiedItemId, AnalysisSessionId, ExtractedExclusionId, CoverageType, RawText)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (unified_id, session_id, first_ext_id, cov_type, raw_text),
                )

                coverages.append(ErectorCoverage(
                    analysis_session_id    = session_id,
                    erector_name           = name,
                    coverage_type          = cov_type,
                    raw_text               = raw_text,
                    extracted_exclusion_id = first_ext_id,
                ))

            results.append(UnifiedItemResult(
                canonical_description = canonical,
                category_id           = category_id,
                coverage              = coverages,
            ))

        self._db.commit()
        log.info(f"  Category '{category_name}': {len(results)} unified items")

        return results


    def _clear_unified_items(self, comparison_id: int) -> None:
        """Remove all unified items + coverage for a comparison (for re-runs)."""

        # Delete coverage first (FK constraint)
        self._db.execute(
            f"""
            DELETE FROM {self._db.schema}.UnifiedItemCoverage
            WHERE UnifiedItemId IN (
                SELECT Id FROM {self._db.schema}.UnifiedItems
                WHERE ComparisonSessionId = ?
            )
            """,
            (comparison_id,),
        )

        self._db.execute(
            f"""
            DELETE FROM {self._db.schema}.UnifiedItems
            WHERE ComparisonSessionId = ?
            """,
            (comparison_id,),
        )

        self._db.commit()
