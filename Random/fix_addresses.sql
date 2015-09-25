BEGIN;

CREATE FUNCTION fix_addresses() RETURNS INTEGER AS
$$
DECLARE
  r RECORD;
  addr BIGINT;
  type TEXT;
  count INTEGER;
BEGIN
  count := 0;
  
  -- Both shared addresses are the same
  FOR r IN SELECT aua.valid, aua.within_city_limits, aua.address_type, 
    au.id as usr, aua.street1, aua.street2, aua.city, aua.county, 
    aua.state, aua.country, aua.post_code, aua.pending, 
    aua.replaces 
    FROM actor.usr au INNER JOIN actor.usr_address aua 
    ON (aua.id = au.mailing_address) 
    WHERE aua.usr != au.id 
    AND au.mailing_address = au.billing_address
	AND au.home_ou in(select id from actor.org_unit where lower(name)~'sulliv')
  LOOP
    INSERT INTO actor.usr_address (valid, within_city_limits, address_type, usr,
      street1, street2, city, county, state, country, post_code, pending, 
      replaces)
    VALUES (r.valid, r.within_city_limits, r.address_type, r.usr, r.street1, 
      r.street2, r.city, r.county, r.state, r.country, r.post_code, r.pending, 
      r.replaces) RETURNING id INTO addr;
    
    UPDATE actor.usr SET mailing_address = addr, billing_address = addr 
    WHERE id = r.usr;
    
    count := count + 1;
  END LOOP;
  
  -- Only one address is shared
  FOREACH type IN ARRAY ARRAY['mailing_address', 'billing_address']
  LOOP
    FOR r IN EXECUTE 'SELECT aua.valid, aua.within_city_limits, 
      aua.address_type, au.id as usr, aua.street1, aua.street2, aua.city, 
      aua.county, aua.state, aua.country, aua.post_code, aua.pending, 
      aua.replaces 
      FROM actor.usr au INNER JOIN actor.usr_address aua 
      ON (aua.id = au.' || type || ') 
      WHERE aua.usr != au.id AND au.home_ou in(select id from actor.org_unit where lower(name)~''sulliv'')'
    LOOP
      INSERT INTO actor.usr_address (valid, within_city_limits, address_type, 
        usr, street1, street2, city, county, state, country, post_code, pending, 
        replaces)
      VALUES (r.valid, r.within_city_limits, r.address_type, r.usr, r.street1, 
        r.street2, r.city, r.county, r.state, r.country, r.post_code, r.pending, 
        r.replaces) RETURNING id INTO addr;
    
      EXECUTE 'UPDATE actor.usr SET ' || type || ' = ' || addr  
        || ' WHERE id = ' || r.usr;
      
      count := count + 1;
    END LOOP;
  END LOOP;
  
  RETURN count;
  
END;
$$ LANGUAGE plpgsql;

SELECT fix_addresses();

DROP FUNCTION fix_addresses();

COMMIT;
