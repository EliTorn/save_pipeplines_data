-- One-time lookup: map CASINO.MENU_ITEMS.ITEMID -> APPLICATION_NAME.
-- Loaded once per pipeline run (cached) and joined into each part that
-- declares LOOKUP_SQL/LOOKUP_KEY_COL/LOOKUP_OUTPUT_COL in events.yaml.
SELECT ITEMID, APPLICATION_NAME
FROM CASINO.MENU_ITEMS
