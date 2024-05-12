import pytest
from unittest.mock import Mock
from self_service.client.unitycatalog import UnityCatalogClient, catalog
from databricks.sdk.service.iam import Group, User, PatchOp, PatchSchema, Patch

def test_create_default_groups():
    # Setup
    workspace_client_mock = Mock()
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=workspace_client_mock, account_client=account_client_mock)
    
    # Mock create_group to return a mock group object with an id attribute
    mock_group_info = Mock()
    mock_group_info.id = "mock_id"
    account_client_mock.groups.create.return_value = mock_group_info
    
    # Expected suffixes
    expected_suffixes = ["read", "readwrite", "writemetadata"]
    catalog_name = "test_catalog"
    
    # Call the method
    result = unity_catalog_client.create_default_groups(catalog_name)
    
    # Assertions
    assert account_client_mock.groups.create.call_count == len(expected_suffixes), "create_group should be called for each suffix"
    for suffix in expected_suffixes:
        expected_display_name = f"{catalog_name}_{suffix}"
        account_client_mock.groups.create.assert_any_call(display_name=expected_display_name)
        assert result[expected_display_name] == "mock_id", f"Group ID for {expected_display_name} should be stored in result"

def test_assign_default_permissions_creates_three_changes():
    # Setup
    workspace_client_mock = Mock()
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=workspace_client_mock, account_client=account_client_mock)
    
    catalog_name = "test_catalog"
    
    # Call the method
    unity_catalog_client.assign_default_permissions(catalog_name)
    
    # Check the call to the API
    workspace_client_mock.grants.update.assert_called_once()
    
    # Extract the 'changes' argument from the API call
    called_args, called_kwargs = workspace_client_mock.grants.update.call_args
    changes = called_kwargs['changes']
    
    # Assertions
    assert len(changes) == 3, "There should be exactly three permissions changes"
    assert all(isinstance(change, catalog.PermissionsChange) for change in changes), "All changes should be instances of PermissionsChange"


def test_delete_catalog_and_groups():
    workspace_client_mock = Mock()
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=workspace_client_mock, account_client=account_client_mock)

    catalog_name = "test_catalog"
    # Create mock Group objects from dictionaries
    mock_groups = [Group.from_dict({'id': 'group1'}), Group.from_dict({'id': 'group2'})]
    account_client_mock.groups.list.return_value = mock_groups

    unity_catalog_client.delete_catalog_and_groups(catalog_name)

    workspace_client_mock.catalogs.delete.assert_called_once_with(name=catalog_name, force=True)
    account_client_mock.groups.list.assert_called_once_with(filter=f"displayName sw '{catalog_name}_'")
    assert account_client_mock.groups.delete.call_count == len(mock_groups)
    for group in mock_groups:
        account_client_mock.groups.delete.assert_any_call(group.id)

def test_find_user_id_found():
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=Mock(), account_client=account_client_mock)
    
    user_display_name = "John Doe"
    mock_users = [User.from_dict({'id': 'user1', 'display_name': user_display_name})]
    account_client_mock.users.list.return_value = mock_users
    
    user_id = unity_catalog_client.find_user_id(user_display_name)
    
    account_client_mock.users.list.assert_called_once_with(filter=f"userName eq '{user_display_name}'")
    assert user_id == 'user1'

def test_find_user_id_not_found():
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=Mock(), account_client=account_client_mock)
    
    user_display_name = "John Doe"
    account_client_mock.users.list.return_value = []
    
    with pytest.raises(ValueError):
        unity_catalog_client.find_user_id(user_display_name)

def test_find_group_id_found():
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=Mock(), account_client=account_client_mock)
    
    catalog_name = "test_catalog"
    access_type = "read"
    mock_groups = [Group.from_dict({'id': 'group1', 'display_name': f'{catalog_name}_{access_type}'})]
    account_client_mock.groups.list.return_value = mock_groups
    
    group_id = unity_catalog_client.find_group_id(catalog_name, access_type)

    account_client_mock.groups.list.assert_called_once_with(filter=f"displayName eq '{catalog_name}_{access_type}'")
    assert group_id == 'group1'

def test_find_group_id_not_found():
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=Mock(), account_client=account_client_mock)
    
    catalog_name = "test_catalog"
    access_type = "read"
    account_client_mock.groups.list.return_value = []
    
    with pytest.raises(ValueError):
        unity_catalog_client.find_group_id(catalog_name, access_type)

def test_grant_access():
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=Mock(), account_client=account_client_mock)
    
    unity_catalog_client.find_user_id = Mock(return_value='user1')
    unity_catalog_client.find_group_id = Mock(return_value='group1')
    
    catalog_name = "test_catalog"
    access_type = "read"
    user_display_name = "John Doe"
    
    unity_catalog_client.grant_access(catalog_name, access_type, user_display_name)
    
    expected_operations = [Patch(op=PatchOp.ADD, path='members', value={'value': 'user1'})]
    expected_schemas = [PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
    
    account_client_mock.groups.patch.assert_called_once_with(
        id='group1',
        operations=expected_operations,
        schemas=expected_schemas
    )

def test_revoke_access():
    account_client_mock = Mock()
    unity_catalog_client = UnityCatalogClient(workspace_client=Mock(), account_client=account_client_mock)
    
    unity_catalog_client.find_user_id = Mock(return_value='user1')
    unity_catalog_client.find_group_id = Mock(return_value='group1')
    
    catalog_name = "test_catalog"
    access_type = "read"
    user_display_name = "John Doe"
    
    unity_catalog_client.revoke_access(catalog_name, access_type, user_display_name)
    
    expected_operations = [Patch(op=PatchOp.REMOVE, path='members', value={'value': 'user1'})]
    expected_schemas = [PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
    
    account_client_mock.groups.patch.assert_called_once_with(
        id='group1',
        operations=expected_operations,
        schemas=expected_schemas
    )