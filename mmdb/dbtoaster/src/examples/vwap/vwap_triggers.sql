BEGIN TRANSACTION;
DROP TRIGGER IF EXISTS maintain_vwap_bcv on bcv;
DROP TRIGGER IF EXISTS maintain_bcv_b2 on b2;
DROP TRIGGER IF EXISTS maintain_bcv_b1 on bids;
DROP TRIGGER IF EXISTS maintain_b2 on bids;
DROP TRIGGER IF EXISTS maintain_bv on bids;

DROP TABLE IF EXISTS bv;
DROP TABLE IF EXISTS b2;
DROP TABLE IF EXISTS bcv;
DROP TABLE IF EXISTS vwap;

CREATE TABLE bv (total_volume decimal);
CREATE TABLE b2 (price decimal);
CREATE TABLE bcv (price decimal, cumsum_volume decimal);
CREATE TABLE vwap (price decimal, volume decimal);

END TRANSACTION;

-- trigger to maintain total_volume in 'bv' view
--
CREATE OR REPLACE FUNCTION maintain_bv() RETURNS TRIGGER AS $maintain_bv$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            UPDATE bv SET total_volume = total_volume + NEW.volume;
        ELSIF (TG_OP = 'UPDATE') THEN
            UPDATE bv SET total_volume = total_volume + (NEW.volume - OLD.volume);
        ELSIF (TG_OP = 'DELETE') THEN
            UPDATE bv SET total_volume = total_volume - OLD.volume;
        END IF;
        RETURN NULL;
    END;
$maintain_bv$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_bv
    AFTER INSERT OR UPDATE OR DELETE ON bids
    FOR EACH ROW EXECUTE PROCEDURE maintain_bv();


-- trigger to maintain distinct prices in 'b2' view
--
CREATE OR REPLACE FUNCTION maintain_b2() RETURNS TRIGGER AS $maintain_b2$
    DECLARE
        price_count integer;
    BEGIN
        price_count := 0;
        IF (TG_OP = 'INSERT') THEN

            SELECT COUNT(*) INTO price_count FROM bids WHERE bids.price = NEW.price;
            IF (price_count = 1) THEN
               INSERT INTO b2 VALUES (NEW.price);
            END IF;

        ELSIF (TG_OP = 'DELETE') THEN

            SELECT COUNT(*) INTO price_count FROM bids WHERE bids.price = OLD.price;
            IF (price_count = 0) THEN
               DELETE FROM b2 WHERE b2.price = OLD.price;
            END IF;

        END IF;
        RETURN NULL;
    END;
$maintain_b2$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_b2
    AFTER INSERT OR DELETE ON bids
    FOR EACH ROW EXECUTE PROCEDURE maintain_b2();


-- trigger to maintain p2, sum(v1) in 'bcv' view from changes in b1
--
-- does not handle price updates, assume prices stay fixed,
-- and only volume changes
CREATE OR REPLACE FUNCTION maintain_bcv_b1() RETURNS TRIGGER AS $maintain_bcv_b1$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            UPDATE bcv
                SET cumsum_volume = cumsum_volume + DELTA.volume
                FROM (SELECT b2.price, NEW.volume as volume FROM b2
                         WHERE b2.price < NEW.price) AS DELTA
                WHERE bcv.price = DELTA.price;

        ELSIF (TG_OP = 'UPDATE') THEN
            UPDATE bcv
                SET cumsum_volume = cumsum_volume + DELTA.volume
                FROM (SELECT b2.price, (NEW.volume - OLD.volume) as volume 
                         FROM b2
                         WHERE b2.price < NEW.price) AS DELTA
                WHERE bcv.price = DELTA.price;

        ELSIF (TG_OP = 'DELETE') THEN
            UPDATE bcv
                SET cumsum_volume = cumsum_volume + DELTA.volume
                FROM (SELECT b2.price, -OLD.volume as volume FROM b2
                        WHERE b2.price < OLD.price) AS DELTA
                WHERE bcv.price = DELTA.price;

        END IF;
        RETURN NULL;
    END;
$maintain_bcv_b1$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_bcv_b1
    AFTER INSERT OR UPDATE OR DELETE on bids
    FOR EACH ROW EXECUTE PROCEDURE maintain_bcv_b1();

-- trigger to maintain bcv from changes in b2
--
CREATE OR REPLACE FUNCTION maintain_bcv_b2() RETURNS TRIGGER AS $maintain_bcv_b2$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            INSERT INTO bcv (price, cumsum_volume)
            SELECT NEW.price, sum(volume) as cumsum_volume
                FROM bids WHERE bids.price > NEW.price;

        ELSIF (TG_OP = 'DELETE') THEN
            DELETE FROM b2 WHERE b2.price = OLD.price;

        END IF;
        RETURN NULL;
    END;
$maintain_bcv_b2$ LANGUAGE plpgsql;

CREATE TRIGGER maintain_bcv_b2
    AFTER INSERT OR DELETE ON b2
    FOR EACH ROW EXECUTE PROCEDURE maintain_bcv_b2();


-- recompute vwap from each change to bcv
CREATE OR REPLACE FUNCTION maintain_vwap_bcv() RETURNS TRIGGER AS $maintain_vwap_bcv$
    DECLARE
        vol_limit decimal;
    BEGIN
        SELECT 0.25*total_volume INTO vol_limit FROM bv;
        IF (TG_OP = 'INSERT') THEN
            IF (vol_limit > NEW.cumsum_volume) THEN
                INSERT INTO vwap (price, volume)
                SELECT bids.price, bids.volume FROM bids
                    WHERE bids.price = NEW.price;
            END IF;

        ELSIF (TG_OP = 'DELETE') THEN
            -- we could wrap this in a conditional if we could get the old
            -- total volume, which would tell us if this deletion was
            -- already in vwap.
            DELETE FROM vwap WHERE vwap.price = OLD.price;

        ELSIF (TG_OP = 'UPDATE') THEN
            -- recompute, i.e. delete all w/ matching prices and reinsert.
            DELETE FROM vwap WHERE price = OLD.price;

            IF (vol_limit > NEW.cumsum_volume) THEN
                INSERT INTO vwap (price, volume)
                SELECT bids.price, bids.volume FROM bids
                    WHERE bids.price = NEW.price;
            END IF;
        END IF;
        RETURN NULL;
    END;
$maintain_vwap_bcv$ LANGUAGE plpgsql;


CREATE TRIGGER maintain_vwap_bcv
    AFTER INSERT OR UPDATE OR DELETE ON bcv
    FOR EACH ROW EXECUTE PROCEDURE maintain_vwap_bcv();
