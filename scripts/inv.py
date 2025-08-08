import mergin

LOGIN="r22overviews"
PASSWORD="MerginOverviews123"
URL="https://app.dev.merginmaps.com"
WS_ID = 10173

client = mergin.MerginClient(
    url=URL,
    login=LOGIN,
    password=PASSWORD
)

client.create_invitation(email="marcel.kocisek@lutraconsulting.co.uk", workspace_id=WS_ID, workspace_role=mergin.common.WorkspaceRole.EDITOR)