from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

# Set up the URL of the Gen3 commons you're working with

# program_name = "ARDaC"
# commons_url = 'https://gen3-iudcc.cis230185.projects.jetstream-cloud.org/'

program_name = "ORIEN"
commons_url = 'https://newdata-test04.cis230185.projects.jetstream-cloud.org/'

# Authenticate using your credentials file (JSON format)
credentials = f"./credentials_{program_name}.json"
auth = Gen3Auth(commons_url, refresh_file=credentials)

# Instantiate the Gen3Submission class to interact with the submission API
submission = Gen3Submission(commons_url, auth)

# Specify the program, project, and the type of node to be deleted

# program = "ARDaC"
# project = "AlcHepNet"
# node_type = "demographic"

program = "ORIEN"
project = "Avatar"
node_type = "study"

# Delete the node of the specified type from the program and project
print(f"Attempting to delete node...")
print(f"Program: {program}")
print(f"Project: {project}")
print(f"Node Type: {node_type}")
response = submission.delete_node(program, project, node_type)
print(f"Response received: {response}")

if response is None:
    print(f"May success nor failed. Please check your website.")
elif response.status_code == 200:
    print(f"Successfully deleted node '{node_type}' from program '{program}' and project '{project}'.")
else:
    print(f"Failed to delete node: {response.status_code} - {response.text}")