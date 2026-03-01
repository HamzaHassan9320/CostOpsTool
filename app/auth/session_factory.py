import boto3

def make_boto3_session(profile_name: str) -> boto3.Session:
    # boto3 automatically uses cached SSO credentials for that profile
    return boto3.Session(profile_name=profile_name)