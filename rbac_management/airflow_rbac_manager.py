import yaml
import argparse
import logging
from google.auth.transport.requests import AuthorizedSession
import google.auth
from requests.exceptions import RequestException
from functools import wraps

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Google Cloud Auth Scope
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

def log_method_call(func):
    """Decorator to log the start and end of a method, including its arguments and return value."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug(f"Starting '{func.__name__}' with args: {args} and kwargs: {kwargs}")
        result = func(*args, **kwargs)
        logger.debug(f"Finished '{func.__name__}' with result: {result}")
        return result
    return wrapper

def flatten_permissions(permission_groups):
    """Flatten grouped permissions (by action) into a list of (action, resource) tuples."""
    flattened_permissions = []
    for action, resources in permission_groups.items():
        for resource in resources:
            flattened_permissions.append((action, resource))
    return flattened_permissions

class GoogleCloudSession:
    """Class responsible for managing Google Cloud authenticated sessions."""

    def __init__(self):
        self.session = self.create_authed_session()

    @staticmethod
    def create_authed_session():
        """Creates an authorized session for interacting with Google Cloud APIs."""
        try:
            credentials, _ = google.auth.default(scopes=[AUTH_SCOPE])
            return AuthorizedSession(credentials)
        except Exception as e:
            logger.error(f"Failed to create authorized session: {e}")
            raise

class AirflowAPI:
    """Class to manage interactions with the Airflow API for roles and users."""

    def __init__(self, airflow_url, session):
        self.airflow_url = airflow_url
        self.session = session

    @log_method_call
    def make_request(self, endpoint, method="GET", data=None):
        """Makes an HTTP request to the Airflow API.
        
        Args:
            endpoint (str): The API endpoint.
            method (str): The HTTP method (GET, POST, PATCH, DELETE).
            data (dict, optional): Data to be sent in the request body.
        
        Returns:
            dict or None: The response data if the request is successful, otherwise None.
        """
        url = f"{self.airflow_url}{endpoint}"
        try:
            if method == "GET":
                response = self.session.get(url)
            elif method == "POST":
                response = self.session.post(url, json=data)
            elif method == "PATCH":
                response = self.session.patch(url, json=data)
            elif method == "DELETE":
                response = self.session.delete(url)
            response.raise_for_status()
            return response.json() if method == "GET" else response
        except RequestException as e:
            if e.response:
                logger.error(f"Request failed: {e.response.status_code}, Response content: {e.response.text}")
            else:
                logger.error(f"Request failed: {e}")
            return None

    @log_method_call
    def create_or_update_role(self, role_name, permission_groups, existing_roles):
        """Creates or updates a role with the specified permissions.
        
        Args:
            role_name (str): The name of the role.
            permission_groups (dict): Permissions grouped by action.
            existing_roles (dict): Dictionary of existing roles.
        
        Returns:
            Response from the Airflow API.
        """
        permissions = flatten_permissions(permission_groups)

        if role_name in existing_roles:
            logger.info(f"Role '{role_name}' already exists, checking for updates.")
            current_permissions = set(tuple(p) for p in existing_roles[role_name].get("permissions", []))
            new_permissions = set(permissions)
            permissions_to_add = new_permissions - current_permissions

            if permissions_to_add:
                logger.info(f"Updating role: {role_name} with new permissions.")
                permission_data = {
                    "name": role_name,
                    "actions": [
                        {"resource": {"name": perm[1]}, "action": {"name": perm[0]}} for perm in permissions_to_add
                    ],
                }
                return self.make_request(f"/auth/fab/v1/roles/{role_name}", method="PATCH", data=permission_data)
        else:
            logger.info(f"Creating role: {role_name}")
            role_data = {
                "name": role_name,
                "actions": [
                    {"resource": {"name": perm[1]}, "action": {"name": perm[0]}} for perm in permissions
                ],
            }
            return self.make_request("/auth/fab/v1/roles", method="POST", data=role_data)

    @log_method_call
    def get_roles(self):
        """Fetches the existing roles from Airflow.
        
        Returns:
            dict: A dictionary of existing roles.
        """
        response = self.make_request("/auth/fab/v1/roles")
        if response and 'roles' in response:
            return {role['name']: role for role in response['roles']}
        return {}

    @log_method_call
    def get_users(self):
        """Fetches the existing users from Airflow.
        
        Returns:
            dict: A dictionary of existing users.
        """
        response = self.make_request("/auth/fab/v1/users")
        if response and 'users' in response:
            return {user['username']: user for user in response['users']}
        return {}

    @log_method_call
    def create_or_update_user(self, user_info, existing_users):
        """Creates or updates a user with the provided user information.
        
        Args:
            user_info (dict): Information about the user.
            existing_users (dict): Dictionary of existing users.
        
        Returns:
            Response from the Airflow API.
        """
        username = user_info.get("username", "")
        user_data = {
            "username": username,
            "first_name": user_info.get("first_name", ""),
            "last_name": user_info.get("last_name", ""),
            "email": user_info.get("email", ""),
            "roles": [{"name": role} for role in user_info.get("roles", [])],
        }
        if username in existing_users:
            logger.info(f"User '{username}' already exists, updating user.")
            return self.make_request(f"/auth/fab/v1/users/{username}", method="PATCH", data=user_data)
        else:
            logger.info(f"Creating user: {username}")
            return self.make_request("/auth/fab/v1/users", method="POST", data=user_data)

    @log_method_call
    def delete_role(self, role_name):
        """Deletes a role by name.
        
        Args:
            role_name (str): The name of the role to delete.
        
        Returns:
            Response from the Airflow API.
        """
        logger.info(f"Deleting role: {role_name}")
        return self.make_request(f"/auth/fab/v1/roles/{role_name}", method="DELETE")

    @log_method_call
    def delete_user(self, username):
        """Deletes a user by username.
        
        Args:
            username (str): The username of the user to delete.
        
        Returns:
            Response from the Airflow API.
        """
        logger.info(f"Deleting user: {username}")
        return self.make_request(f"/auth/fab/v1/users/{username}", method="DELETE")

def load_yaml_config(yaml_file):
    """Loads the YAML configuration file.
    
    Args:
        yaml_file (str): Path to the YAML configuration file.
    
    Returns:
        dict: Parsed YAML configuration.
    """
    try:
        with open(yaml_file, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Failed to load YAML file: {e}")
        raise

def delete_missing_entities(existing_entities, yaml_entities, delete_function, entity_type, no_delete=False):
    """Deletes entities (roles/users) that are missing from the YAML configuration, if deletion is not disabled.
    
    Args:
        existing_entities (dict): Existing entities from Airflow.
        yaml_entities (list): Entities defined in the YAML configuration.
        delete_function (function): Function to call to delete an entity.
        entity_type (str): Type of entity ('role' or 'user').
        no_delete (bool): If True, deletion is disabled.
    """
    if no_delete:
        logger.info(f"Deletion of {entity_type}s is disabled.")
        return

    yaml_names = {entity['name'] for entity in yaml_entities if 'name' in entity} if entity_type == "role" else \
                 {entity['username'] for entity in yaml_entities if 'username' in entity}
    for entity_name in existing_entities:
        if entity_name not in yaml_names:
            logger.info(f"Deleting {entity_type}: {entity_name}")
            delete_function(entity_name)

def main(yaml_file, no_delete):
    """Main function that handles user and role management in Airflow.
    
    Args:
        yaml_file (str): Path to the YAML configuration file.
        no_delete (bool): If True, disables deletion of roles and users.
    """
    logger.info(f"Starting the RBAC management process with YAML config: {yaml_file}")

    config = load_yaml_config(yaml_file)

    airflow_url = config.get("web_server_url")
    if not airflow_url:
        logger.error("Airflow web server URL is not provided in the configuration.")
        return

    try:
        session = GoogleCloudSession().session
        api = AirflowAPI(airflow_url, session)
    except Exception as e:
        logger.error(f"Error creating an authorized session: {e}")
        return

    existing_roles = api.get_roles()
    existing_users = api.get_users()

    roles = config.get("roles", [])
    for role in roles:
        api.create_or_update_role(role['name'], role['permissions'], existing_roles)

    users = config.get("users", [])
    for user in users:
        api.create_or_update_user(user, existing_users)

    delete_missing_entities(existing_roles, roles, api.delete_role, "role", no_delete)
    delete_missing_entities(existing_users, users, api.delete_user, "user", no_delete)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Airflow RBAC users and roles using a YAML configuration file.")
    parser.add_argument("--rbac_config", required=True, type=str, help="Path to the YAML configuration file")
    parser.add_argument("--no-delete", action="store_true", help="Disable deletion of roles and users")
    args = parser.parse_args()

    main(args.rbac_config, args.no_delete)
