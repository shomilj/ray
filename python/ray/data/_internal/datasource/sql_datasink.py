from typing import Callable, Iterable

from ray.data._internal.datasource.sql_datasource import Connection, _connect
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink


class SQLDatasink(Datasink[None]):

    _MAX_ROWS_PER_WRITE = 128

    def __init__(self, sql: str, connection_factory: Callable[[], Connection]):
        self.sql = sql
        self.connection_factory = connection_factory

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        with _connect(self.connection_factory) as cursor:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)

                values = []
                for row in block_accessor.iter_rows(public_row_format=False):
                    values.append(tuple(row.values()))
                    assert len(values) <= self._MAX_ROWS_PER_WRITE, len(values)
                    if len(values) == self._MAX_ROWS_PER_WRITE:
                        self._execute_batch(cursor, values)
                        values = []

                if values:
                    self._execute_batch(cursor, values)

    def _execute_batch(self, cursor, values):
        """Execute a batch insert using a single query with multiple VALUES clauses.
        
        This is significantly faster than executemany, which essentially loops
        through individual execute statements. The performance improvement is most
        noticeable with network databases (PostgreSQL, MySQL) where round-trip
        overhead is significant.
        """
        if not values:
            return
            
        # For safety, if we can't parse the SQL or it doesn't look like an INSERT with VALUES,
        # fall back to executemany
        sql_upper = self.sql.upper()
        if "VALUES" not in sql_upper:
            cursor.executemany(self.sql, values)
            return
            
        try:
            # Extract the base INSERT statement (everything before VALUES)
            # Handle case-insensitive split
            values_index = sql_upper.find("VALUES")
            base_sql = self.sql[:values_index].strip()
            values_template = self.sql[values_index + 6:].strip()  # 6 = len("VALUES")
            
            # Remove parentheses and count placeholders
            values_template = values_template.strip()
            if values_template.startswith("(") and values_template.endswith(")"):
                values_template = values_template[1:-1]
            
            # Detect placeholder style and count them
            # Common placeholders: %s (PostgreSQL/MySQL), ? (SQLite), :1 (Oracle)
            if "%s" in values_template:
                placeholder = "%s"
                num_placeholders = values_template.count("%s")
            elif "?" in values_template:
                placeholder = "?"
                num_placeholders = values_template.count("?")
            else:
                # Unknown placeholder style, fall back to executemany
                cursor.executemany(self.sql, values)
                return
            
            if num_placeholders == 0:
                # No placeholders found, fall back to executemany
                cursor.executemany(self.sql, values)
                return
            
            # Create the VALUES clause with multiple rows
            values_clause = ", ".join(["(" + ", ".join([placeholder] * num_placeholders) + ")"] * len(values))
            
            # Flatten the values list for the execute statement
            flattened_values = [item for row in values for item in row]
            
            # Execute the single query with all values
            full_sql = f"{base_sql} VALUES {values_clause}"
            cursor.execute(full_sql, flattened_values)
            
        except Exception:
            # If anything goes wrong with our optimization, fall back to executemany
            # This ensures we don't break existing functionality
            cursor.executemany(self.sql, values)