CREATE TABLE IF NOT EXISTS external_links (
    link TEXT NOT NULL,
    is_homepage BOOLEAN
);

CREATE INDEX idx_link_data ON external_links USING GIN (link);
CREATE INDEX idx_is_homepage ON external_links (is_homepage);

CREATE TABLE IF NOT EXISTS aggregated_links (
    primary_link TEXT NOT NULL,
    frequency INTEGER NOT NULL,
    subsections JSONB NOT NULL,
    country TEXT,
    category TEXT,
    is_ad_based BOOLEAN
);

CREATE INDEX idx_primary_link ON aggregated_links (primary_link);
CREATE INDEX idx_frequency ON aggregated_links (frequency);
CREATE INDEX idx_subsections ON aggregated_links USING GIN (subsections);
CREATE INDEX idx_country ON aggregated_links (country);
CREATE INDEX idx_category ON aggregated_links USING GIN (category);
CREATE INDEX idx_is_ad_based ON aggregated_links (is_ad_based);