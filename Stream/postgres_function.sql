-- Add updated_at column if not exists
ALTER TABLE shipment_events
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

-- Function to set updated_at automatically
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger BEFORE UPDATE
CREATE TRIGGER trigger_set_updated_at
BEFORE UPDATE ON shipment_events
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();