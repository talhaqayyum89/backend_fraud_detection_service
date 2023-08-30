from config import config
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

def get_sharepoint_context_using_user():
 
    # Get sharepoint credentials
    sharepoint_url = config.sharepoint_url
    user_name = config.user_name
    password = config.password

    # Initialize the client credentials
    user_credentials = UserCredential(user_name, password)

    # create client context object
    ctx = ClientContext(sharepoint_url).with_credentials(user_credentials)

    return ctx


def upload_to_sharepoint(file_name):

    sp_url = config.sp_url 
    print(sp_url)
    ctx = get_sharepoint_context_using_user()

    target_folder = ctx.web.get_folder_by_server_relative_url(sp_url)
    with open(file_name, 'rb') as content_file:
        file_content = content_file.read()
        target_folder.upload_file(file_name, file_content).execute_query()