import sys
import os

if len(sys.argv) != 2:
    print("Root directory is required")
    exit()

root_directory = sys.argv[1]
print(f"Deploying LinkedIn / Indeed Jobs in root directory {root_directory}")

# Assume you have some deployment logic here
# For example, you can invoke your existing deployment script or commands

# Change directory to your Airflow DAG directory
dag_directory = os.path.join(root_directory, 'airflow/dags')
os.chdir(dag_directory)

# Run your Airflow DAG deployment command
# Modify this based on your actual deployment command
os.system(f"airflow dags trigger -c '{{\"key\": \"value\"}}' Final")

print("Airflow DAG deployment completed.")

# Walk the entire directory structure recursively
ignore_folders = ['__pycache__', '.ipynb_checkpoints']

for (directory_path, directory_names, file_names) in os.walk(root_directory):
    # Get just the last/final folder name in the directory path
    base_name = os.path.basename(directory_path)

    # Skip any folders we want to ignore
    if base_name in ignore_folders:
        continue

    # An app.toml file in the folder is our indication that this folder contains
    # a snowcli Snowpark App
    if not "app.toml" in file_names:
        continue

    # Next determine what type of app it is
    app_type = "unknown"
    if "app.sql" in file_names:
        app_type = "procedure"
    elif "app.py" in file_names:
        app_type = "function"
    else:
        print(f"Skipping unknown app type in folder {directory_path}")
        continue

    # Finally deploy the app with the snowcli tool
    print(f"Found {app_type} app in folder {directory_path}")
    print(f"Calling snowcli to deploy the {app_type} app")
    os.chdir(directory_path)
    # snow login will update the app.toml file with the correct path to the snowsql config file
    os.system(f"snow login -c {root_directory}/.devcontainer/config -C dev")
    os.system(f"snow {app_type} create")
