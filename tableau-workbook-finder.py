import tableauserverclient as TSC

# Tableau Cloud details
TABLEAU_SERVER = "your companies tableau server url" 
SITE_ID = "your companies site id" 
PAT_NAME = "your pat name"
PAT_SECRET = "your pat secret"



# Authenticate
auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, SITE_ID)
server = TSC.Server(TABLEAU_SERVER, use_server_version=True)


# Look for a project (folder) based on name search
def list_projects():
    with server.auth.sign_in(auth):
        projects = list(TSC.Pager(server.projects))

        print("List of Projects:")
        for proj in projects:
            #print(f"- {proj.name} (ID: {proj.id})")
            if proj.name.startswith("DEV"):
                 print(f" - {proj.name} (ID: {proj.id})")

# Look for a workbook based on name search
def find_workbook():
    with server.auth.sign_in(auth):
        all_workbooks = list(TSC.Pager(server.workbooks))

        print(f"Workbooks in Project:")
        for wb in all_workbooks:
            if (wb.name.startswith("Events") or wb.name.startswith("ABC-Events")): # Use this to search for a specific workbook in your project 
                print(f"- {wb.name}")
                

def main():

    list_projects()
    find_workbook()

if __name__ == "__main__":
    main() 