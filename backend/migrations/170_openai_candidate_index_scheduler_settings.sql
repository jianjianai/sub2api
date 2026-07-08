INSERT INTO settings (key, value)
VALUES
    ('openai_candidate_index_scheduler_enabled', 'false'),
    ('openai_candidate_index_scheduler_page_size', ''),
    ('openai_candidate_index_scheduler_max_scan', '')
ON CONFLICT (key) DO NOTHING;
