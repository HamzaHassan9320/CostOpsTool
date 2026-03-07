from app.llm.router import route


def test_route_parse_analyze_command():
    parsed = route('/analyze profile=dev-sso')
    assert parsed.intent == 'analyze'
    assert parsed.profile_name == 'dev-sso'
    assert parsed.action == 'optimization.run_scan'


def test_route_parse_project_command():
    parsed = route('/project smart-comms')
    assert parsed.intent == 'set_project'
    assert parsed.project_name == 'smart-comms'


def test_route_parse_heuristic_analyze_profile():
    parsed = route('please analyze idle nat gateway savings for this account with profile finops-prod')
    assert parsed.intent == 'analyze'
    assert parsed.profile_name == 'finops-prod'
    assert parsed.action == 'optimization.run_scan'


def test_route_parse_rescan_heuristic():
    parsed = route('can you rescan now')
    assert parsed.intent == 'rescan'
