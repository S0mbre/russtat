# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.rsengine
# @brief EMISS data retrieving and processing engine.
import xml.etree.ElementTree as ET
import requests
import os, sys
import json
import re
from datetime import datetime as dt, timedelta
from multiprocessing import Pool
from globs import DEBUGGING

URL_EMISS_LIST = 'https://fedstat.ru/opendata/list.xml'
XML_NS = {'message': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message", 
        'common': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common",
        'compact': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/compact",
        'cross': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/cross",
        'generic': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/generic",
        'query': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/query",
        'structure': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/structure",
        'utility': "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/utility",
        'xsi': "http://www.w3.org/2001/XMLSchema-instance"}

# --------------------------------------------------------------- #

def is_iterable(obj):
    if isinstance(obj, str): return False
    try:
        _ = iter(obj)
        return True
    except:
        return False

# --------------------------------------------------------------- # 

class Russtat:

    def __init__(self, root_folder='', update_list=False, connection_timeout=10):        
        self.root_folder = root_folder
        self.datasets = []
        self._iter = None
        self.connection_timeout = connection_timeout
        self.update_dataset_list(overwrite=update_list, loadfromjson='list_json.json' if not update_list else '')

    def __iter__(self):
        self._iter = self.datasets.__iter__()
        return self._iter

    def next(self):
        if not self.datasets:
            raise StopIteration
        return next(self._iter)

    def __len__(self):
        return len(self.datasets)

    def __bool__(self):
        return bool(self.__len__())

    def __getitem__(self, key):
        if isinstance(key, str):
            for ds in self.datasets:
                if ds['title'].lower() == key.lower():
                    return ds
            raise IndexError 
        elif isinstance(key, int):            
            return self.datasets[key]
        elif isinstance(key, slice):
            return self.datasets[key.start:key.stop:key.step]
        raise TypeError

    def update_dataset_list(self, xmlfilename='list.xml', xml_only=True, overwrite=False, del_xml=True, 
                            save2json='list_json.json', loadfromjson='list_json.json'):
        self.datasets = []

        if loadfromjson:
            try:
                json_file = os.path.abspath(os.path.join(self.root_folder, loadfromjson))
                with open(json_file, 'r', encoding='utf-8') as infile:
                    self.datasets = json.load(infile)
                self._report(f'Loaded from JSON ({json_file}): {len(self.datasets)} datasets')
                return
            except Exception as err:
                self._report(f"{err}   Importing from XML...")
                self.update_dataset_list(xmlfilename, xml_only, overwrite, save2json, None)
            
        outputfile = os.path.abspath(os.path.join(self.root_folder, xmlfilename))
        if not os.path.exists(outputfile) or overwrite:       
            try:
                os.remove(outputfile)
                self._report(f'Deleted existing XML ({outputfile})')
            except Exception as err:
                self._report(err)                
            try:
                res = requests.get(URL_EMISS_LIST, timeout=self.connection_timeout)
                if not res: 
                    self._report(f'Could not retrieve dataset list from {URL_EMISS_LIST}')
                    return

                with open(outputfile, 'wb') as outfile:
                    outfile.write(res.content)
                self._report(f'Downloaded XML from {URL_EMISS_LIST} to {outputfile}')

            except Exception as err:
                self._report(err)
                return
        
        tree = ET.parse(outputfile, ET.XMLParser(encoding='utf-8'))
        root_el = tree.getroot()

        for item in root_el.find('meta').iter('item'):
            if xml_only and item.find('format').text != 'xml':
                continue
            self.datasets.append({child.tag: child.text.strip('"').strip() for child in item})
        self._report(f'Loaded from XML ({outputfile}): {len(self.datasets)} datasets')

        if del_xml:
            try:
                os.remove(outputfile)
                self._report(f'Deleted XML ({outputfile})')
            except Exception as err:
                self._report(err)

        if save2json:
            try:
                json_file = os.path.abspath(os.path.join(self.root_folder, save2json))
                with open(json_file, 'w', encoding='utf-8') as outfile:
                    json.dump(self.datasets, outfile, ensure_ascii=False, indent=4)
                self._report(f'Saved to JSON ({json_file})')
            except Exception as err:
                self._report(err)

    def find_datasets(self, pattern, regex=False, case_sense=False, fullmatch=False):
        results = []
        if regex:
            regexp = re.compile(pattern) if case_sense else re.compile(pattern, re.I)

        if not self.datasets: self.update_dataset_list()

        for item in self.datasets:
            if not 'title' in item: continue
            title = item['title']
            if regex:
                if (fullmatch and regexp.fullmatch(title)) or (not fullmatch and regexp.search(title)):
                    results.append(item)
            else:
                if (fullmatch and \
                    ((case_sense and title == pattern) or (not case_sense and title.lower() == pattern.lower()))) or \
                (not fullmatch and \
                    ((case_sense and pattern in title) or (not case_sense and pattern.lower() in title.lower()))):
                    results.append(item)
        self._report(f"Found {len(results)} matches for query '{pattern}'")
        return results

    def _report(self, message, force_print=False, file=sys.stdout, end='\n', flush=False):
        if force_print or DEBUGGING:
            print(message, end=end, file=file, flush=flush)

    def _get_text(self, node, tags=None, default='', ns=XML_NS, strip=True):
        if node is None: return default
        if tags:
            if is_iterable(tags):
                for tag in tags:
                    node = node.find(tag, ns)
                    if node is None: return default
            else:
                node = node.find(tags, ns)
                if node is None: return default
        return node.text.strip() if strip else node.text
    
    def _get_attr(self, node, attr, tags=None, default='', ns=XML_NS, strip=True):
        if node is None: return default
        if tags:
            if is_iterable(tags):
                for tag in tags:
                    node = node.find(tag, ns)
                    if node is None: return default
            else:
                node = node.find(tags, ns)
                if node is None: return default    
        sub = node.get(attr)
        if sub is None: return default        
        return sub.strip() if strip else sub

    def _get_codes(self, ds_rootnode):
        codelists = ds_rootnode.find('message:CodeLists', XML_NS)
        d_codes = {}

        for item in codelists.iterfind('structure:CodeList', XML_NS):
            name = self._get_attr(item, 'id')
            d_codes[name] = {'name': self._get_text(item, 'structure:Name'), 
                            'values': [(self._get_attr(code, 'value'), 
                                        self._get_text(code, 'structure:Description')) \
                                        for code in item.iterfind('structure:Code', XML_NS)]}
        return d_codes

    def _get_data(self, ds_rootnode, codes, max_row=-1):
        n = 0
        dataset = ds_rootnode.find('message:DataSet', XML_NS)
        if not dataset: return []
        data = []
        for item in dataset.iterfind('generic:Series', XML_NS):
            
            try:
                key = item.find('generic:SeriesKey', XML_NS).find('generic:Value', XML_NS)
                key_concept = self._get_attr(key, 'concept')
                key_key = self._get_attr(key, 'value')
                classifier, cl = ('', '')
                
                for code in codes:
                    if code == key_concept:
                        classifier = codes[code]['name']
                        for val in codes[code]['values']:
                            if val[0] == key_key:
                                cl = val[1]
                                break
                        break
                        
                per, ei = ('', '')
                for attr in item.find('generic:Attributes', XML_NS).iterfind('generic:Value', XML_NS):
                    concept = self._get_attr(attr, 'concept')
                    val = self._get_attr(attr, 'value')
                    if concept == 'EI':
                        ei = val
                    elif concept == 'PERIOD':
                        per = val
                obs = item.find('generic:Obs', XML_NS)
                try:
                    tim = int(self._get_text(obs, 'generic:Time', '0'))
                except ValueError:
                    tim = 0
                try:
                    val = float(self._get_attr(obs, 'value', 'generic:ObsValue', '0.0'))
                except ValueError:
                    val = 0.0
                data.append((classifier, cl, ei, per, tim, val))
                n += 1
                if max_row > 0 and n > max_row: break

            except Exception as err:
                self._report(err)
                break

        return data

    def get_one(self, dataset, xmlfilename='auto', overwrite=True, del_xml=True, 
                save2json='auto', loadfromjson='auto', on_dataset=None, on_dataset_kwargs=None):

        def json_hook(d):
            for k in d:
                if k in ('prepared', 'next', 'updated'):     
                    d.update({k: dt.strptime(d[k], '%Y-%m-%d %H:%M:%S')})
            return d

        if loadfromjson is None or loadfromjson == 'auto':
            if isinstance(dataset, str):
                datasets = self.find_datasets(dataset)
                if not datasets:
                    self._report(f"No datasets match query '{dataset}'")
                    return None
                dataset = datasets[0]
            elif isinstance(dataset, int):
                try:
                    dataset = self[dataset]
                except Exception as err:
                    self._report(err)
                    return None
            elif not isinstance(dataset, dict):
                self._report(f"Bad data type for 'dataset': {type(dataset)}")
                return None
        
        if loadfromjson:
            if loadfromjson == 'auto':
                loadfromjson = os.path.join(self.root_folder,
                                            dataset.get('identifier', 'dataset') + '.json')
            
            ds = None
            try:
                with open(os.path.abspath(loadfromjson), 'r', encoding='utf-8') as infile:
                    ds = json.load(infile, object_hook=json_hook)             
            except Exception as err:
                self._report(f"{err}   Importing from XML...")
                return self.get_one(dataset, xmlfilename, overwrite, del_xml, save2json, None, on_dataset, on_dataset_kwargs)
            else:
                self._report(f'Loaded from JSON ({loadfromjson})')
                if on_dataset: 
                    if on_dataset_kwargs:
                        on_dataset(ds, **on_dataset_kwargs)
                    else:
                        on_dataset(ds)
                return ds

        if not 'link' in dataset:
            self._report('Dataset has no "link" object!')
            return None
        if dataset.get('format', '') != 'xml':
            self._report('Dataset must be in XML format!')
            return None

        if xmlfilename == 'auto':
            xmlfilename = dataset.get('identifier', 'dataset') + '.xml'

        outputfile = os.path.abspath(os.path.join(self.root_folder, xmlfilename))
        if not os.path.exists(outputfile) or overwrite:       
            try:
                os.remove(outputfile)
                self._report(f'Deleted existing XML ({outputfile})')
            except Exception as err:
                self._report(err)                
            try:
                res = requests.get(dataset['link'], timeout=self.connection_timeout)
                if not res: 
                    self._report(f"Could not retrieve dataset from {dataset['link']}")
                    return None

                with open(outputfile, 'wb') as outfile:
                    outfile.write(res.content)
                self._report(f"Downloaded XML from {dataset['link']} to {outputfile}")

            except Exception as err:
                self._report(err)
                return None

        ds = {'prepared': None, 'id': -1, 'agency_id': -1, 'codes': {}, 
              'full_name': '', 'unit': '', 'periodicity': {'value': '', 'releases': '', 'next': None}, 
              'data_range': (-1, -1), 'updated': None, 'methodology': '', 'agency_name': '', 'agency_dept': '', 
              'classifier': {'id': '', 'path': ''}, 'prepared_by': {'name': '', 'contacts': ''}, 'data': []}

        try:
            tree = ET.parse(outputfile, ET.XMLParser(encoding='utf-8'))
            ds_rootnode = tree.getroot()

            # Header
            node_hdr = ds_rootnode.find('message:Header', XML_NS)        
            ds['prepared'] = dt.fromisoformat(self._get_text(node_hdr, 'message:Prepared', '1900-01-01')) - timedelta(hours=3)
            ds['id'] = self._get_text(node_hdr, 'message:DataSetID')
            ds['agency_id'] = self._get_text(node_hdr, 'message:DataSetAgency')

            # Codes
            ds['codes'] = self._get_codes(ds_rootnode)

            # Description
            node_desc = ds_rootnode.find('message:Description', XML_NS).find('message:Indicator', XML_NS)
            ds['full_name'] = self._get_attr(node_desc, 'name')
            ds['unit'] = self._get_attr(node_desc, 'value', ['message:Units', 'message:Unit'])
            ds['periodicity']['value'] = self._get_attr(node_desc, 'value', ['message:Periodicities', 'message:Periodicity'])
            ds['periodicity']['releases'] = self._get_attr(node_desc, 'releases', ['message:Periodicities', 'message:Periodicity'])
            ds['periodicity']['next'] = dt.strptime(self._get_attr(node_desc, 'next-release', ['message:Periodicities', 'message:Periodicity'], '01.01.1900'), '%d.%m.%Y') - timedelta(hours=3)
            ds['data_range'] = tuple(int(self._get_attr(node_desc, x, 'message:DataRange', '0')) for x in ('start', 'end'))
            ds['updated'] = dt.fromisoformat(self._get_attr(node_desc, 'value', 'message:LastUpdate', '1900-01-01')) - timedelta(hours=3)
            ds['methodology'] = self._get_attr(node_desc, 'value', 'message:Methodology')
            ds['agency_name'] = self._get_attr(node_desc, 'value', 'message:Organization')
            ds['agency_dept'] = self._get_attr(node_desc, 'value', 'message:Department')
            ds['classifier']['id'] = self._get_attr(node_desc, 'id', ['message:Allocations', 'message:Allocation'])
            ds['classifier']['path'] = self._get_text(node_desc, ['message:Allocations', 'message:Allocation', 'message:Name'])
            ds['prepared_by']['name'] = self._get_text(node_desc, ['message:Responsible', 'message:Name'])
            ds['prepared_by']['contacts'] = self._get_text(node_desc, ['message:Responsible', 'message:Contacts'])
            ds['data'] = self._get_data(ds_rootnode, ds['codes'])

            if save2json:
                if save2json == 'auto':
                    save2json = dataset.get('identifier', 'dataset') + '.json'
                try:
                    json_file = os.path.abspath(os.path.join(self.root_folder, save2json))
                    with open(json_file, 'w', encoding='utf-8') as outfile:
                        json.dump(ds, outfile, ensure_ascii=False, indent=4, default=str)
                    self._report(f'Saved to JSON ({json_file})')
                except Exception as err:
                    self._report(err)

            if del_xml:
                try:
                    os.remove(outputfile)
                    self._report(f'Deleted XML ({outputfile})')
                except Exception as err:
                    self._report(err)

            if on_dataset: 
                if on_dataset_kwargs:
                    on_dataset(ds, **on_dataset_kwargs)
                else:
                    on_dataset(ds)

        except Exception as err:
            self._report(err)
            if del_xml:
                try:
                    os.remove(outputfile)
                    self._report(f'Deleted XML ({outputfile})')
                except Exception as err2:
                    self._report(err2)
            return None

        return ds

    def get_many(self, datasets=None, xmlfilenames='auto', overwrite=True, del_xml=True, 
              save2json='auto', loadfromjson='auto',
              processes='auto', wait=True, on_dataset=None, on_dataset_kwargs=None,
              on_results_ready=None, on_error=None, on_stopcheck=None):

        args = []

        if datasets is None and loadfromjson != 'auto' and not loadfromjson is None:

            if is_iterable(loadfromjson):
                for json_file in loadfromjson:
                    args.append((None, None, False, False, None, json_file, on_dataset, on_dataset_kwargs))
            else:
                args.append((None, None, False, False, None, loadfromjson, on_dataset, on_dataset_kwargs))

        else:

            if not self.datasets: self.update_dataset_list()

            if datasets is None:
                datasets = self.datasets
            elif isinstance(datasets, str):
                datasets = self.find_datasets(datasets)
            elif isinstance(datasets, int):
                datasets = [self.datasets[datasets]]
            elif is_iterable(datasets):
                if len(datasets) == 0:
                    self._report('Empty datasets parameter!', True)
                    return None          
                if isinstance(datasets[0], int) or isinstance(datasets[0], str):
                    datasets = [self[k] for k in datasets]            
            else:
                self._report('Bad type: datasets', True)
                return None

            if not datasets:
                self._report('No datasets matching your request.', True)
                return None

            # prepare args for worker function        
            for i, ds in enumerate(datasets):
                try:                
                    if is_iterable(xmlfilenames):
                        xmlfilename = xmlfilenames[i]
                    else:
                        xmlfilename = xmlfilenames                    
                    
                    if is_iterable(save2json):
                        save2json_ = save2json[i]            
                    else:
                        save2json_ = save2json

                    if is_iterable(loadfromjson):
                        loadfromjson_ = loadfromjson[i]            
                    else:
                        loadfromjson_ = loadfromjson
                    
                    args.append((ds, xmlfilename, overwrite, del_xml, save2json_, loadfromjson_, on_dataset, on_dataset_kwargs))
                    
                except Exception as err:
                    self._report(err, True)
                    return None

        if processes == 'auto': processes = None
        
        with Pool(processes=processes) as pool:
            try:
                result = pool.starmap_async(self.get_one, args, callback=on_results_ready, error_callback=on_error)
                pool.close()
                if wait: pool.join()
                return result
            except Exception as err:
                self._report(err, True)
                return None