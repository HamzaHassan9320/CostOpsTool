from app.auth.session_factory import make_boto3_session

def validate_profile(profile_name: str) -> dict:
    sess = make_boto3_session(profile_name)
    sts = sess.client("sts")
    ident = sts.get_caller_identity()
    return {
        "account": ident.get("Account"),
        "arn": ident.get("Arn"),
        "user_id": ident.get("UserId"),
    }