UPDATE datasets SET 
	name = trim(regexp_replace(name, '\s+', ' ', 'g')),
	description = trim(regexp_replace(description, '\s+', ' ', 'g'));