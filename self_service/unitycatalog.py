class UnityCatalog:
    def __init__(self, unity_catalog_client):
        self.client = unity_catalog_client

    def create_catalog(self, catalog_name):
        """ Create a new catalog and associated groups with default privileges. """
        catalog_info = self.client.create_catalog(catalog_name)
        self.client.create_default_groups(catalog_name)
        self.client.assign_default_permissions(catalog_name)
        print(f"Created catalog '{catalog_name}' and default security groups.")
        return catalog_info

    def delete_catalog(self, catalog_name):
        """ Delete a catalog and its associated groups. """
        self.client.delete_catalog_and_groups(catalog_name)
        print(f"Deleted catalog '{catalog_name}' and its associated groups.")

    def grant_access(self, catalog_name: str, access_type: str, user_display_name: str):
        """High-level method to grant access to a user based on access type and user's display name."""
        self.client.grant_access(catalog_name, access_type, user_display_name)
        print(f"Access granted to '{user_display_name}' for '{catalog_name}' with '{access_type}' access.")
    
    def revoke_access(self, catalog_name: str, access_type: str, user_display_name: str):
        """High-level method to revoke access from a user based on access type and user's display name."""
        self.client.revoke_access(catalog_name, access_type, user_display_name)
        print(f"Access revoked from '{user_display_name}' for '{catalog_name}' with '{access_type}' access.")