--
-- PostgreSQL database dump
--

-- Dumped from database version 13.0
-- Dumped by pg_dump version 13.0

-- Started on 2020-10-05 15:43:54

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP DATABASE russtat;
--
-- TOC entry 3093 (class 1262 OID 16394)
-- Name: russtat; Type: DATABASE; Schema: -; Owner: -
--

CREATE DATABASE russtat WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE = 'Russian_Russia.1251';


\connect russtat

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 231 (class 1255 OID 16561)
-- Name: add_data(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_data(dataset_json text, time_format text DEFAULT 'YYYY-MM-DD HH24-MI-SS'::text, OUT n_added integer, OUT last_data_id bigint, OUT dataset_id integer) RETURNS record
    LANGUAGE plpgsql
    AS $$
declare
	data_json jsonb;
	prep_time timestamp with time zone;
	last_updated timestamp with time zone;
	next_update timestamp with time zone; 
	data_rec json;
	codes_id integer;
	codevals_id integer;
	units_id integer;
	periods_id integer;
	cnt1 bigint;
	cnt2 bigint;
	r text;
begin
	n_added := 0;
	last_data_id := -1;
	dataset_id := -1;
	
	data_json := dataset_json::jsonb;
	
	prep_time := to_timestamp(data_json->>'prepared', time_format);
	last_updated := to_timestamp(data_json->>'updated', time_format);
	next_update := to_timestamp(data_json->'periodicity'->>'next', time_format);
	
	-- add / update dataset
	
	/*
	raise notice 'ARGS TO add_dataset:\n'
				 'prep_time=%\nlast_updated=%\nnext_update=%\n'
				 'ds_id=%\nfullname=%\nagency_id=%\nagency_name=%\n'
				 'agency_dept=%\ncodes=%\nunit=%\n'
				 'periodicity=%\nthis_release=%\ndata_range=%\n'
				 'description=%\nclassifier_id=%\nclassifier_path=%\n'
				 'prep_by=%\nprep_contacts=%', 
				  prep_time, last_updated, next_update,
				  data_json->>'id', data_json->>'full_name',
				  data_json->>'agency_id', data_json->>'agency_name',
				  data_json->>'agency_dept', jsonb_pretty(data_json->'codes'),
				  data_json->>'unit', data_json->'periodicity'->>'value',
				  data_json->'periodicity'->>'releases',
				  array[cast(data_json->'data_range'->>0 as integer), 
						cast(data_json->'data_range'->>1 as integer)], 
				  data_json->>'methodology', 
				  data_json->'classifier'->>'id',
				  data_json->'classifier'->>'path',
				  data_json->'prepared_by'->>'name',
				  data_json->'prepared_by'->>'contacts';
	return;
	*/
	
	select into dataset_id public.add_dataset(prep_time, last_updated, next_update,
								  data_json->>'id', data_json->>'full_name',
								  data_json->>'agency_id', data_json->>'agency_name',
								  data_json->>'agency_dept', data_json->'codes',
								  data_json->>'unit', data_json->'periodicity'->>'value',
								  data_json->'periodicity'->>'releases',
								  array[cast(data_json->'data_range'->>0 as integer), 
										cast(data_json->'data_range'->>1 as integer)], 
								  data_json->>'methodology', 
								  data_json->'classifier'->>'id',
								  data_json->'classifier'->>'path',
								  data_json->'prepared_by'->>'name',
								  data_json->'prepared_by'->>'contacts');
	if dataset_id = -1 then
		raise notice '! Unable to add or update dataset!';
		return;
	end if;
	
	-- add observations
	select count(*) into cnt1 from obs;
	
	for data_rec in select * from jsonb_array_elements(data_json->'data')
	loop
		-- find code
		begin
			select id into strict codes_id from codes
			where lower(name) = lower(data_rec->>0) limit 1;
		exception
			when NO_DATA_FOUND then
			insert into codes(name) values (data_rec->>0)
				on conflict on constraint codes_unique1 do
				update set name = excluded.name 
				returning id into codes_id;
		end;
				
		-- find codeval
		begin
			select id into strict codevals_id from codevals
			where code_id = codes_id and lower(name) = lower(data_rec->>1) limit 1;
		exception 
			when NO_DATA_FOUND then
			insert into codevals(code_id, name) values (codes_id, data_rec->>1)
				on conflict on constraint codevals_unique do
				update set code_id = excluded.code_id, name = excluded.name
				returning id into codevals_id;
		end;
				
		-- find unit
		begin
			select id into strict units_id from units
			where lower(val) = lower(data_rec->>2) limit 1;
		exception
			when NO_DATA_FOUND then
			insert into units(val) values (data_rec->>2)
				on conflict on constraint units_unique do
				update set val = excluded.val 
				returning id into units_id;
		end;
				
		-- find period
		begin
			select id into strict periods_id from periods
			where lower(val) = lower(data_rec->>3) limit 1;
		exception
			when NO_DATA_FOUND then
			insert into periods(val) values (data_rec->>3)
				on conflict on constraint periods_unique do
				update set val = excluded.val 
				returning id into periods_id;
		end;
				
		-- insert data record
		
		/*
		raise notice 'dataset_id=%, code_id=%, unit_id=%, '
					 'period_id=%, obs_year=%, obs_val=%',
					 dataset_id, codevals_id, units_id, periods_id, 
					 data_rec->>4, data_rec->>5;
		continue;
		*/
		
		insert into obs(dataset_id, code_id, unit_id, period_id, obs_year, obs_val) 
			values (dataset_id, codevals_id, units_id, periods_id, 
					cast(data_rec->>4 as integer), cast(data_rec->>5 as real))
			on conflict on constraint obs_unique1 do
			update set dataset_id = excluded.dataset_id, code_id = excluded.code_id,
				unit_id = excluded.unit_id, period_id = excluded.period_id,
				obs_year = excluded.obs_year, obs_val = excluded.obs_val 
			returning id into last_data_id;
				
	end loop;
	
	select count(*) into cnt2 from obs;
	n_added := cnt2 - cnt1;
	
end;
$$;


--
-- TOC entry 233 (class 1255 OID 16554)
-- Name: add_dataset(timestamp with time zone, timestamp with time zone, timestamp with time zone, text, text, text, text, text, jsonb, text, text, text, integer[], text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.add_dataset(prep_time timestamp with time zone, last_updated timestamp with time zone, next_update timestamp with time zone, ds_id text, fullname text, agency_id text, agency_name text, agency_dept text, codes jsonb, unit text, periodicity text, this_release text, data_range integer[], description text, classifier_id text, classifier_path text, prep_by text, prep_contacts text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
declare
	ag_id_ integer;
	dept_id_ integer;
	class_id_ integer;
	units_id_ integer;
	periods_id_ integer;
	codes_id_ integer;
	codename text;
	js_codeval jsonb;
	ds_id_ integer;
begin
	/*
	raise notice 'Agency ID = %\nAgency name = %\nDep = %\n'
				 'Cls ID = %\nCls path = %\nUnit = %\nPeriod = %\n'
				 'Prep = %\nUpdated = %\nNext = %\nRange = %\n'
				 'DS ID = %\nName = %\nDesc = %\n'
				 'By = %\nContact = %', agency_id, agency_name, agency_dept,
				 classifier_id, classifier_path, unit, periodicity,
				 prep_time, last_updated, next_update, data_range,
				 ds_id, fullname, description, prep_by, prep_contacts;
	return -1;
	*/

	-- agency
	insert into agencies(ag_id, name)
		values (agency_id, agency_name)
		on conflict on constraint agencies_unique1 do 
		update set name = excluded.name, ag_id = excluded.ag_id 
		returning id into ag_id_;	
	
	-- department
	insert into departments(agency_id, name)
		values (ag_id_, agency_dept)
		on conflict on constraint depts_unique1 do
		update set name = excluded.name 
		returning id into dept_id_;
		
	-- classifier
	insert into classifier(class_id, name)
		values (classifier_id, classifier_path)
		on conflict on constraint classifier_unique1 do
		update set name = excluded.name 
		returning id into class_id_;
		
	-- units
	insert into units(val) values (unit)
		on conflict on constraint units_unique do
		update set val = excluded.val 
		returning id into units_id_;
		
	-- periodicity
	insert into periods(val) values (periodicity)
		on conflict on constraint periods_unique do 
		update set val = excluded.val 
		returning id into periods_id_;
		
	-- codes
	-- {'code_id': {'name': 'code_name', 'values': [['val_id', 'name'], [...], ...]}, {...}, ...}
	for codename in select * from jsonb_object_keys(codes)
	loop
		insert into codes(name) values (codes->codename->>'name')
			on conflict on constraint codes_unique1 do
			update set name = excluded.name 
			returning id into codes_id_;

		for js_codeval in select * from jsonb_array_elements(codes->codename->'values')
		loop
			insert into codevals(code_id, val_id, name) 
				values (codes_id_, js_codeval->>0, js_codeval->>1)
				on conflict on constraint codevals_unique do
				update set name = excluded.name;
		end loop;
	end loop;
	
	-- datasets
	insert into datasets(prep_time, updated_time, next_update_time,
						ds_id, agency_id, dept_id, name, period_id,
						unit_id, range_start, range_end, class_id,
						description, prep_by, prep_contact, code_id)
		values (prep_time, last_updated, next_update,
			   ds_id, ag_id_, dept_id_, fullname, periods_id_,
			   units_id_, data_range[1], data_range[2], class_id_,
			   description, prep_by, prep_contacts, codes_id_)
		on conflict on constraint datasets_unique1 do
		update set prep_time = excluded.prep_time, updated_time = excluded.updated_time, 
			next_update_time = excluded.next_update_time, ds_id = excluded.ds_id, 
			agency_id = excluded.agency_id, dept_id = excluded.dept_id,
			name = excluded.name, period_id = excluded.period_id, unit_id = excluded.unit_id,
			range_start = excluded.range_start, range_end = excluded.range_end,
			class_id = excluded.class_id, description = excluded.description, 
			prep_by = excluded.prep_by, prep_contact = excluded.prep_contact, code_id = excluded.code_id			
		returning id into ds_id_;
		
	return ds_id_;
	
end;
$$;


--
-- TOC entry 232 (class 1255 OID 16614)
-- Name: clear_all(boolean, boolean); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.clear_all(fullclear boolean DEFAULT false, restart_seq boolean DEFAULT true)
    LANGUAGE plpgsql
    AS $$
begin
	delete from obs;
	delete from datasets;
	delete from codevals;
	delete from codes;

	if restart_seq then
		alter sequence obs_id_seq restart;
		alter sequence datasets_id_seq restart;
		alter sequence codevals_id_seq restart;
		alter sequence codes_id_seq restart;
	end if;

	if fullclear then
		delete from classifier;
		delete from units;
		delete from periods;
		delete from departments;
		delete from agencies;
		if restart_seq then
			alter sequence classifier_id_seq restart;
			alter sequence units_id_seq restart;
			alter sequence periods_id_seq restart;
			alter sequence departments_id_seq restart;
			alter sequence agencies_id_seq restart;
		end if;
	end if;
end;
$$;


--
-- TOC entry 229 (class 1255 OID 16530)
-- Name: resort_classifier_fn(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.resort_classifier_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
	class_row RECORD;
	last_pos integer;
	parent_part text;
	pid integer;
begin
	-- raise notice 'Searching all records with parent_id = -1...';
	for class_row in 
		select id as id_, name as name_ 
		from classifier where parent_id = -1
	loop
		-- raise notice 'Found item: %', class_row.name_;
		last_pos := length(class_row.name_) - position('/' in reverse(class_row.name_));
		parent_part := rtrim(left(class_row.name_, last_pos));
		if length(parent_part) = length(class_row.name_) then
			update classifier set parent_id = null 
			where classifier.id = class_row.id_;
			continue;
		end if;
		select id into pid from classifier where name = parent_part;
		if found then
			-- raise notice 'Found parent: %', pid;
			update classifier set parent_id = pid 
			where classifier.id = class_row.id_;
		end if;
	end loop;	
	
	return null;
end;
$$;


--
-- TOC entry 230 (class 1255 OID 16559)
-- Name: txt2jsonb(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.txt2jsonb(text_json text, OUT jsonb_obj jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
begin
	jsonb_obj := to_jsonb(text_json);
end;
$$;


SET default_table_access_method = heap;

--
-- TOC entry 201 (class 1259 OID 16397)
-- Name: agencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.agencies (
    id integer NOT NULL,
    ag_id character varying(32),
    name text NOT NULL
);


--
-- TOC entry 200 (class 1259 OID 16395)
-- Name: agencies_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.agencies ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.agencies_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 205 (class 1259 OID 16419)
-- Name: classifier; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.classifier (
    id integer NOT NULL,
    class_id character varying(32),
    name text NOT NULL,
    parent_id integer DEFAULT '-1'::integer
);


--
-- TOC entry 204 (class 1259 OID 16417)
-- Name: classifier_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.classifier ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.classifier_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 207 (class 1259 OID 16429)
-- Name: codes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.codes (
    id integer NOT NULL,
    name text NOT NULL
);


--
-- TOC entry 206 (class 1259 OID 16427)
-- Name: codes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.codes ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.codes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 209 (class 1259 OID 16436)
-- Name: codevals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.codevals (
    id integer NOT NULL,
    code_id integer,
    val_id character varying(32) DEFAULT ''::character varying,
    name text NOT NULL
);


--
-- TOC entry 208 (class 1259 OID 16434)
-- Name: codevals_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.codevals ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.codevals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 215 (class 1259 OID 16462)
-- Name: datasets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.datasets (
    id integer NOT NULL,
    prep_time timestamp with time zone,
    updated_time timestamp with time zone,
    next_update_time timestamp with time zone,
    ds_id text,
    agency_id integer,
    dept_id integer,
    name text NOT NULL,
    unit_id integer,
    range_start smallint,
    range_end smallint,
    class_id integer,
    description text,
    prep_by text,
    prep_contact text,
    code_id integer,
    period_id integer
);


--
-- TOC entry 214 (class 1259 OID 16460)
-- Name: datasets_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.datasets ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.datasets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 203 (class 1259 OID 16407)
-- Name: departments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.departments (
    id integer NOT NULL,
    agency_id integer,
    name text NOT NULL
);


--
-- TOC entry 202 (class 1259 OID 16405)
-- Name: departments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.departments ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.departments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 217 (class 1259 OID 16492)
-- Name: obs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.obs (
    id bigint NOT NULL,
    dataset_id integer,
    code_id integer,
    unit_id integer,
    period_id integer,
    obs_year integer,
    obs_val real
);


--
-- TOC entry 216 (class 1259 OID 16490)
-- Name: obs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.obs ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.obs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 213 (class 1259 OID 16455)
-- Name: periods; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.periods (
    id integer NOT NULL,
    val character varying(256) NOT NULL
);


--
-- TOC entry 212 (class 1259 OID 16453)
-- Name: periods_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.periods ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.periods_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 211 (class 1259 OID 16448)
-- Name: units; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.units (
    id integer NOT NULL,
    val character varying(256) NOT NULL
);


--
-- TOC entry 210 (class 1259 OID 16446)
-- Name: units_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.units ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.units_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 2912 (class 2606 OID 16404)
-- Name: agencies agencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agencies
    ADD CONSTRAINT agencies_pkey PRIMARY KEY (id);


--
-- TOC entry 2914 (class 2606 OID 16626)
-- Name: agencies agencies_unique1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.agencies
    ADD CONSTRAINT agencies_unique1 UNIQUE (name) INCLUDE (name);


--
-- TOC entry 2920 (class 2606 OID 16426)
-- Name: classifier classifier_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.classifier
    ADD CONSTRAINT classifier_pkey PRIMARY KEY (id);


--
-- TOC entry 2922 (class 2606 OID 16628)
-- Name: classifier classifier_unique1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.classifier
    ADD CONSTRAINT classifier_unique1 UNIQUE (name) INCLUDE (name);


--
-- TOC entry 2924 (class 2606 OID 16433)
-- Name: codes codes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codes
    ADD CONSTRAINT codes_pkey PRIMARY KEY (id);


--
-- TOC entry 2926 (class 2606 OID 16630)
-- Name: codes codes_unique1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codes
    ADD CONSTRAINT codes_unique1 UNIQUE (name) INCLUDE (name);


--
-- TOC entry 2928 (class 2606 OID 16440)
-- Name: codevals codevals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codevals
    ADD CONSTRAINT codevals_pkey PRIMARY KEY (id);


--
-- TOC entry 2930 (class 2606 OID 16624)
-- Name: codevals codevals_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codevals
    ADD CONSTRAINT codevals_unique UNIQUE (code_id, val_id);


--
-- TOC entry 2940 (class 2606 OID 16469)
-- Name: datasets datasets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_pkey PRIMARY KEY (id);


--
-- TOC entry 2942 (class 2606 OID 16598)
-- Name: datasets datasets_unique1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_unique1 UNIQUE (ds_id) INCLUDE (ds_id);


--
-- TOC entry 2916 (class 2606 OID 16411)
-- Name: departments departments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.departments
    ADD CONSTRAINT departments_pkey PRIMARY KEY (id);


--
-- TOC entry 2918 (class 2606 OID 16635)
-- Name: departments depts_unique1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.departments
    ADD CONSTRAINT depts_unique1 UNIQUE (agency_id, name);


--
-- TOC entry 2944 (class 2606 OID 16496)
-- Name: obs obs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.obs
    ADD CONSTRAINT obs_pkey PRIMARY KEY (id);


--
-- TOC entry 2946 (class 2606 OID 16543)
-- Name: obs obs_unique1; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.obs
    ADD CONSTRAINT obs_unique1 UNIQUE (dataset_id, code_id, unit_id, period_id, obs_year, obs_val);


--
-- TOC entry 2936 (class 2606 OID 16459)
-- Name: periods periods_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.periods
    ADD CONSTRAINT periods_pkey PRIMARY KEY (id);


--
-- TOC entry 2938 (class 2606 OID 16640)
-- Name: periods periods_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.periods
    ADD CONSTRAINT periods_unique UNIQUE (val) INCLUDE (val);


--
-- TOC entry 2932 (class 2606 OID 16452)
-- Name: units units_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.units
    ADD CONSTRAINT units_pkey PRIMARY KEY (id);


--
-- TOC entry 2934 (class 2606 OID 16642)
-- Name: units units_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.units
    ADD CONSTRAINT units_unique UNIQUE (val) INCLUDE (val);


--
-- TOC entry 2957 (class 2620 OID 16531)
-- Name: classifier classifier_on_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER classifier_on_update AFTER INSERT OR UPDATE ON public.classifier FOR EACH ROW EXECUTE FUNCTION public.resort_classifier_fn();


--
-- TOC entry 2948 (class 2606 OID 16441)
-- Name: codevals codevals_fk1; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.codevals
    ADD CONSTRAINT codevals_fk1 FOREIGN KEY (code_id) REFERENCES public.codes(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2949 (class 2606 OID 16470)
-- Name: datasets datasets_fk1; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_fk1 FOREIGN KEY (agency_id) REFERENCES public.agencies(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2950 (class 2606 OID 16475)
-- Name: datasets datasets_fk2; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_fk2 FOREIGN KEY (dept_id) REFERENCES public.departments(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2951 (class 2606 OID 16480)
-- Name: datasets datasets_fk3; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_fk3 FOREIGN KEY (unit_id) REFERENCES public.units(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2952 (class 2606 OID 16485)
-- Name: datasets datasets_fk4; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datasets
    ADD CONSTRAINT datasets_fk4 FOREIGN KEY (class_id) REFERENCES public.classifier(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2947 (class 2606 OID 16412)
-- Name: departments departments_fk1; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.departments
    ADD CONSTRAINT departments_fk1 FOREIGN KEY (agency_id) REFERENCES public.agencies(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2953 (class 2606 OID 16497)
-- Name: obs obs_fk1; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.obs
    ADD CONSTRAINT obs_fk1 FOREIGN KEY (dataset_id) REFERENCES public.datasets(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2954 (class 2606 OID 16502)
-- Name: obs obs_fk2; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.obs
    ADD CONSTRAINT obs_fk2 FOREIGN KEY (code_id) REFERENCES public.codevals(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2955 (class 2606 OID 16507)
-- Name: obs obs_fk3; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.obs
    ADD CONSTRAINT obs_fk3 FOREIGN KEY (unit_id) REFERENCES public.units(id) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- TOC entry 2956 (class 2606 OID 16512)
-- Name: obs obs_fk4; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.obs
    ADD CONSTRAINT obs_fk4 FOREIGN KEY (period_id) REFERENCES public.periods(id) ON UPDATE CASCADE ON DELETE SET NULL;


-- Completed on 2020-10-05 15:43:54

--
-- PostgreSQL database dump complete
--

