from databricks.sdk import WorkspaceClient

class Metadata:
    def __init__(self, full_name: str):
        self.client = WorkspaceClient()
        self.full_name = full_name
        self.securable_type = self._infer_securable_type()

    def _infer_securable_type(self):
        """
        Infer the securable type based on the number of elements in the full name.
        """
        parts = self.full_name.split('.')
        if len(parts) == 1:
            return 'catalog'
        elif len(parts) == 2:
            return 'schema'
        elif len(parts) == 3:
            return 'table'
        else:
            raise ValueError("Invalid full_name format")

    def add_comment(self, comment: str):
        """
        Add or update a comment on a securable object.
        """
        update_method = getattr(self.client, f"{self.securable_type}s").update
        return update_method(self.full_name, comment=comment)

    def remove_comment(self):
        """
        Remove a comment from a securable object.
        """
        update_method = getattr(self.client, f"{self.securable_type}s").update
        return update_method(self.full_name, comment="")

    def add_property(self, key: str, value: str):
        """
        Add or update a property on a securable object.
        """
        update_method = getattr(self.client, f"{self.securable_type}s").update
        return update_method(self.full_name, properties={key: value})

    def remove_property(self, key: str):
        """
        Remove a property from a securable object.
        """
        update_method = getattr(self.client, f"{self.securable_type}s").update
        current_properties = getattr(self.client, f"{self.securable_type}s").get(full_name=self.full_name).properties
        if key in current_properties:
            del current_properties[key]
            return update_method(self.full_name, properties=current_properties)

