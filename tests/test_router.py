from app.llm.router import route


def test_route_parse_analyze_command():
    parsed = route('/analyze profile=dev-sso')
    assert parsed.intent == 'analyze'
    assert parsed.profile_name == 'dev-sso'
    assert parsed.action == 'optimization.run_scan'
    assert parsed.account_scope == 'current'
    assert parsed.target_account_id is None


def test_route_parse_analyse_command_alias():
    parsed = route('/analyse profile=dev-sso scope=all')
    assert parsed.intent == 'analyze'
    assert parsed.profile_name == 'dev-sso'
    assert parsed.account_scope == 'all'


def test_route_parse_project_command():
    parsed = route('/project smart-comms')
    assert parsed.intent == 'set_project'
    assert parsed.project_name == 'smart-comms'


def test_route_parse_heuristic_analyze_profile():
    parsed = route('please analyze idle nat gateway savings for this account with profile finops-prod')
    assert parsed.intent == 'analyze'
    assert parsed.profile_name == 'finops-prod'
    assert parsed.action == 'optimization.run_scan'


def test_route_parse_heuristic_analyse_profile_with_trailing_bracket():
    parsed = route('analyse CostOptimizationHub_ReadOnly-546377338878]')
    assert parsed.intent == 'analyze'
    assert parsed.profile_name == 'CostOptimizationHub_ReadOnly-546377338878'
    assert parsed.account_scope == 'current'


def test_route_parse_heuristic_bare_account_id_under_analyze_intent():
    parsed = route('analyze 546377338878')
    assert parsed.intent == 'analyze'
    assert parsed.account_scope == 'account'
    assert parsed.target_account_id == '546377338878'


def test_route_parse_rescan_heuristic():
    parsed = route('can you rescan now')
    assert parsed.intent == 'rescan'


def test_route_parse_retry_phrase():
    parsed = route('try again')
    assert parsed.intent == 'retry'


def test_route_detect_target_service_ec2():
    parsed = route('run ec2 analysis in this account')
    assert parsed.intent == 'analyze'
    assert parsed.target_service == 'ec2'


def test_route_parse_analyze_scope_all():
    parsed = route('/analyze profile=dev-sso scope=all')
    assert parsed.intent == 'analyze'
    assert parsed.profile_name == 'dev-sso'
    assert parsed.account_scope == 'all'
    assert parsed.target_account_id is None


def test_route_parse_analyze_scope_specific_account():
    parsed = route('/analyze profile=dev-sso scope=account:123456789012')
    assert parsed.intent == 'analyze'
    assert parsed.account_scope == 'account'
    assert parsed.target_account_id == '123456789012'


def test_route_parse_analyze_scope_account_name():
    parsed = route('/analyze profile=dev-sso scope=account:sc-awslogging1')
    assert parsed.intent == 'analyze'
    assert parsed.account_scope == 'account'
    assert parsed.target_account_id == 'sc-awslogging1'
