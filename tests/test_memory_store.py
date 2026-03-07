from app.memory import store
from app.memory.store import ProjectMemory


def test_memory_store_create_and_upsert(tmp_path, monkeypatch):
    data_dir = tmp_path / 'data'
    memory_file = data_dir / 'project_memory.json'

    monkeypatch.setattr(store, 'DATA_DIR', data_dir)
    monkeypatch.setattr(store, 'MEMORY_FILE', memory_file)

    loaded = store.load_memory_store()
    assert loaded['version'] == 2
    assert loaded['projects'] == {}

    store.upsert_project(
        ProjectMemory(
            project_name='smart-comms',
            aws_profile_name='dev-sso',
            account_id='123456789012',
            athena_database='cur_db',
            athena_table='cur_table',
            athena_workgroup='primary',
            athena_output_s3='s3://bucket/prefix',
            athena_profile_name='payer-sso',
            athena_region='us-east-1',
        )
    )

    project = store.get_project('smart-comms')
    assert project is not None
    assert project.aws_profile_name == 'dev-sso'
    assert project.account_id == '123456789012'
    assert project.athena_database == 'cur_db'
    assert memory_file.exists()


def test_memory_store_list_projects_sorted(tmp_path, monkeypatch):
    data_dir = tmp_path / 'data'
    memory_file = data_dir / 'project_memory.json'

    monkeypatch.setattr(store, 'DATA_DIR', data_dir)
    monkeypatch.setattr(store, 'MEMORY_FILE', memory_file)

    store.upsert_project(ProjectMemory(project_name='zeta'))
    store.upsert_project(ProjectMemory(project_name='alpha'))

    assert store.list_projects() == ['alpha', 'zeta']


def test_memory_store_touch_project_updates_last_used(tmp_path, monkeypatch):
    data_dir = tmp_path / 'data'
    memory_file = data_dir / 'project_memory.json'

    monkeypatch.setattr(store, 'DATA_DIR', data_dir)
    monkeypatch.setattr(store, 'MEMORY_FILE', memory_file)

    store.upsert_project(ProjectMemory(project_name='costops'))
    first = store.get_project('costops')
    assert first is not None

    store.touch_project('costops')
    second = store.get_project('costops')
    assert second is not None
    assert second.last_used_at >= first.last_used_at
    assert second.updated_at >= first.updated_at


def test_memory_store_ignores_legacy_edp_field(tmp_path, monkeypatch):
    data_dir = tmp_path / 'data'
    memory_file = data_dir / 'project_memory.json'

    monkeypatch.setattr(store, 'DATA_DIR', data_dir)
    monkeypatch.setattr(store, 'MEMORY_FILE', memory_file)

    data_dir.mkdir(parents=True, exist_ok=True)
    memory_file.write_text(
        '{"version":1,"projects":{"legacy":{"project_name":"legacy","edp_percent":12.5,"athena_database":"db"}}}',
        encoding='utf-8',
    )

    project = store.get_project('legacy')
    assert project is not None
    assert project.project_name == 'legacy'
    assert project.athena_database == 'db'
