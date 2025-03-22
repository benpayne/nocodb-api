from __future__ import annotations
from typing import TYPE_CHECKING, Any
from pathlib import Path

from nocodb.Column import Column

if TYPE_CHECKING:
    from nocodb.Table import Table


class Record:
    def __init__(self, table: "Table", **kwargs) -> None:
        self.table = table
        self.noco_db = table.noco_db

        self.record_id = kwargs["Id"]
        self.metadata = kwargs

    def link_record(self, column: Column, link_record: "Record") -> bool:
        path = (
            f"tables/{self.table.table_id}/links/"
            + f"{column.column_id}/records/{self.record_id}"
        )
        r = self.noco_db.call_noco(
            path=path, method="POST", json={"Id": link_record.record_id}
        )

        return r.json()

    def link_records(self, column: Column, link_records: list["Record"]) -> bool:
        path = (
            f"tables/{self.table.table_id}/links/"
            + f"{column.column_id}/records/{self.record_id}"
        )
        r = self.noco_db.call_noco(
            path=path, method="POST", json=[{"Id": l.record_id} for l in link_records]
        )

        return r.json()

    def get_linked_records(self, column: Column) -> list[Record]:
        """
        Get records linked to this record through the given column.

        Args:
            column: The column representing the link

        Returns:
            List of linked records
        """
        try:
            path = (
                f"tables/{self.table.table_id}/links/"
                + f"{column.column_id}/records/{self.record_id}"
            )
            r = self.noco_db.call_noco(path=path)

            # Extract record IDs from the response
            record_ids = []

            try:
                response_data = r.json()

                # Handle different response formats
                if "list" in response_data:
                    if not response_data["list"]:
                        return []
                    elif isinstance(response_data["list"], list):
                        record_ids = [l.get("Id") for l in response_data["list"] if "Id" in l]
                    elif isinstance(response_data["list"], dict) and "Id" in response_data["list"]:
                        record_ids = [response_data["list"]["Id"]]
                elif "Id" in response_data:
                    record_ids = [response_data["Id"]]
                else:
                    # Check for other possible formats
                    for field in response_data:
                        if isinstance(response_data[field], dict) and "Id" in response_data[field]:
                            record_ids.append(response_data[field]["Id"])

                # If record IDs are not found in common formats, try a more general approach
                if not record_ids and isinstance(response_data, dict):
                    # Try to find any field that contains an "Id"
                    for field, value in response_data.items():
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict) and "Id" in item:
                                    record_ids.append(item["Id"])
                        elif isinstance(value, dict) and "Id" in value:
                            record_ids.append(value["Id"])

                # Check if any record IDs were found
                if not record_ids:
                    # If we have the foreign key directly in our record, try to use it
                    fk_name = f"{column.title}_id"
                    if fk_name in self.metadata and self.metadata[fk_name]:
                        record_ids = [self.metadata[fk_name]]
            except Exception as e:
                # If we can't parse the response, see if we have a direct foreign key
                fk_name = f"{column.title}_id"
                if fk_name in self.metadata and self.metadata[fk_name]:
                    record_ids = [self.metadata[fk_name]]

            # Filter out None values and ensure all IDs are integers
            record_ids = [int(id) for id in record_ids if id is not None]

            if not record_ids:
                return []

            # Get the linked table
            if hasattr(column, "linked_table_id"):
                linked_table = self.noco_db.get_table(column.linked_table_id)
            else:
                # Try to determine the linked table from the column name
                linked_table_name = column.title
                linked_table = None
                base = self.table.get_base()
                for table in base.get_tables():
                    if table.title == linked_table_name:
                        linked_table = table
                        break

                if not linked_table:
                    # Try removing "s" if the name ends with it (to handle pluralization)
                    if linked_table_name.endswith("s"):
                        linked_table_name = linked_table_name[:-1]
                        for table in base.get_tables():
                            if table.title == linked_table_name:
                                linked_table = table
                                break

                # If we still can't find the linked table, raise an exception
                if not linked_table:
                    raise Exception(f"Could not determine linked table for column {column.title}")

            # Get the linked records
            return linked_table.get_records_by_id(record_ids)

        except Exception as e:
            # Try a simpler approach - if the link is set up properly in metadata
            # Look for direct foreign key field
            fk_name = f"{column.title}_id"
            if fk_name in self.metadata and self.metadata[fk_name]:
                record_id = self.metadata[fk_name]

                # Try to determine the linked table
                base = self.table.get_base()
                linked_table = None

                for table in base.get_tables():
                    if table.title == column.title:
                        linked_table = table
                        break

                if not linked_table:
                    raise Exception(f"Could not find linked table: {column.title}")

                # Get the linked record
                return linked_table.get_records_by_id([record_id])

            # If direct foreign key field didn't work, rethrow the original exception
            raise e

    def get_value(self, field: str) -> Any:
        return self.get_values([field])[field]

    def get_column_value(self, column: Column) -> Any:
        return self.get_value(column.title)

    def get_values(self, fields: list[str] | None = None, include_system: bool = True) -> dict:
        if not include_system:
            cols = [c.title for c in self.table.get_columns(include_system)]
            if fields:
                fields = [f for f in fields if f in cols]
            else:
                fields = cols

        field_str = ",".join(fields) if fields else ""
        r = self.noco_db.call_noco(
            path=f"tables/{self.table.table_id}/records/{self.record_id}",
            params={"fields": field_str}
        )
        return r.json()

    def get_attachments(self, field: str, encoding: str = "utf-8") -> list[str]:
        value_list = self.get_value(field)
        if not isinstance(value_list, list):
            raise Exception("Invalid field value")

        return [
            self.noco_db.get_file(p["signedUrl"], encoding=encoding)
            for p in value_list
        ]

    def update(self, **kwargs) -> Record:
        kwargs["Id"] = self.record_id
        r = self.noco_db.call_noco(
            path=f"tables/{self.table.table_id}/records",
            method="PATCH",
            json=kwargs,
        )
        return self.table.get_record(record_id=r.json()["Id"])

    def upload_attachment(
        self, field: str, filepath: Path, mimetype: str = ""
    ) -> Record:
        value = self.get_value(field=field) or []
        value.append(self.noco_db.upload_file(
            filepath=filepath, mimetype=mimetype))

        return self.update(**{field: value})
