CREATE FUNCTION sr_reject_published_definition_version_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status = 'PUBLISHED' THEN
        RAISE EXCEPTION 'published task definition versions are immutable'
            USING ERRCODE = '23514';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE TRIGGER tr_sr_definition_version_immutable
BEFORE UPDATE OR DELETE ON sr_task_definition_version
FOR EACH ROW
EXECUTE FUNCTION sr_reject_published_definition_version_mutation();
