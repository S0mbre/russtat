# -*- coding: utf-8 -*-
# --------------------------------------------------------------- #

import xml.etree.ElementTree as ET
import requests
import os
import json
import re
from datetime import datetime as dt
from multiprocessing import Pool, TimeoutError

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

    def __init__(self, root_folder='', update_list=False):        
        self.root_folder = root_folder
        self.datasets = []
        self._iter = None
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

    def update_dataset_list(self, xmlfilename='list.xml', xml_only=True, overwrite=False, save2json='list_json.json', loadfromjson='list_json.json'):
        self.datasets = []

        if loadfromjson:
            try:
                with open(os.path.join(self.root_folder, loadfromjson), 'r', encoding='utf-8') as infile:
                    self.datasets = json.load(infile)
                print(f'Loaded from JSON: {len(self.datasets)} datasets')
                return
            except Exception as err:
                print(err)
                print('Importing from XML...')
                self.update_dataset_list(xmlfilename, xml_only, overwrite, save2json, None)
            
        outputfile = os.path.join(self.root_folder, xmlfilename)
        if not os.path.exists(outputfile) or overwrite:       
            try:
                os.remove(outputfile)
                print('Deleted existing XML')
            except:
                pass
            try:
                res = requests.get(URL_EMISS_LIST)
                if not res: 
                    print(f'Could not retrieve dataset list from {URL_EMISS_LIST}')
                    return

                with open(outputfile, 'wb') as outfile:
                    outfile.write(res.content)
                print(f'Downloaded XML from {URL_EMISS_LIST}')

            except Exception as err:
                print(err)
                return
        
        tree = ET.parse(outputfile, ET.XMLParser(encoding='utf-8'))
        root_el = tree.getroot()

        for item in root_el.find('meta').iter('item'):
            if xml_only and item.find('format').text != 'xml':
                continue
            self.datasets.append({child.tag: child.text.strip('"').strip() for child in item})
        print(f'Loaded from XML: {len(self.datasets)} datasets')

        if save2json:
            with open(os.path.join(self.root_folder, save2json), 'w', encoding='utf-8') as outfile:
                json.dump(self.datasets, outfile, ensure_ascii=False, indent=4)
            print('Saved to JSON')

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
        print(f"Found {len(results)} matches for query '{pattern}'")
        return results

    def _get_codes(self, ds_rootnode):
        codelists = ds_rootnode.find('message:CodeLists', XML_NS)
        d_codes = {}

        for item in codelists.iterfind('structure:CodeList', XML_NS):
            name = item.get('id')
            d_codes[name] = {'name': item.find('structure:Name', XML_NS).text, 
                            'values': [(code.get('value'), code.find('structure:Description', XML_NS).text) \
                                        for code in item.iterfind('structure:Code', XML_NS)]}
        return d_codes

    def _get_data(self, ds_rootnode, codes, max_row=-1):
        n = 0
        dataset = ds_rootnode.find('message:DataSet', XML_NS)
        data = []
        for item in dataset.iterfind('generic:Series', XML_NS):
            
            try:
                key = item.find('generic:SeriesKey', XML_NS).find('generic:Value', XML_NS)
                key_concept = key.get('concept')
                key_key = key.get('value')
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
                    concept = attr.get('concept')
                    val = attr.get('value')
                    if concept == 'EI':
                        ei = val
                    elif concept == 'PERIOD':
                        per = val
                obs = item.find('generic:Obs', XML_NS)
                try:
                    tim = int(obs.find('generic:Time', XML_NS).text)
                except ValueError:
                    tim = obs.find('generic:Time', XML_NS).text
                try:
                    val = float(obs.find('generic:ObsValue', XML_NS).get('value'))
                except ValueError:
                    val = obs.find('generic:ObsValue', XML_NS).get('value')
                data.append((classifier, cl, ei, per, tim, val))       
                n += 1
                if max_row > 0 and n > max_row: break

            except Exception as err:
                print(err)
                break

        return data

    def parse_dataset(self, dataset, xmlfilename='dataset.xml', overwrite=True, save2json='', loadfromjson=''):

        def json_hook(d):
            for k in d:
                if k in ('prepared', 'next', 'updated'):     
                    d.update({k: dt.strptime(d[k], '%Y-%m-%d %H:%M:%S')})     
            return d

        if loadfromjson:
            try:
                with open(os.path.join(self.root_folder, loadfromjson), 'r', encoding='utf-8') as infile:
                    ds = json.load(infile, object_hook=json_hook)
                    print('Loaded from JSON')
                    return ds
            except Exception as err:
                print(err)
                print('Importing from XML...')
                return self.parse_dataset(dataset, xmlfilename, overwrite, save2json, None)

        if not 'link' in dataset:
            print('Dataset has no "link" object!')
            return None
        if not 'format' in dataset:
            print('Dataset has no "format" object!')
            return None
        if dataset['format'] != 'xml':
            print('Dataset must be in XML format!')
            return None

        outputfile = os.path.join(self.root_folder, xmlfilename)
        if not os.path.exists(outputfile) or overwrite:       
            try:
                os.remove(outputfile)
                print('Deleted existing XML')
            except:
                pass
            try:
                res = requests.get(dataset['link'])
                if not res: 
                    print(f"Could not retrieve dataset from {dataset['link']}")
                    return None

                with open(outputfile, 'wb') as outfile:
                    outfile.write(res.content)
                print(f"Downloaded XML from {dataset['link']}")

            except Exception as err:
                print(err)
                return None

        ds = {'prepared': None, 'id': -1, 'agency_id': -1, 'codes': {}, 
              'full_name': '', 'unit': '', 'periodicity': {'value': '', 'releases': '', 'next': None}, 
              'data_range': (-1, -1), 'updated': None, 'methodology': '', 'agency_name': '', 'agency_dept': '', 
              'classifier': {'id': '', 'path': ''}, 'prepared_by': {'name': '', 'contacts': ''}, 'data': []}

        tree = ET.parse(outputfile, ET.XMLParser(encoding='utf-8'))
        ds_rootnode = tree.getroot()

        try:

            # Header
            node_hdr = ds_rootnode.find('message:Header', XML_NS)        
            ds['prepared'] = dt.fromisoformat(node_hdr.find('message:Prepared', XML_NS).text)
            ds['id'] = node_hdr.find('message:DataSetID', XML_NS).text
            ds['agency_id'] = node_hdr.find('message:DataSetAgency', XML_NS).text

            # Codes
            ds['codes'] = self._get_codes(ds_rootnode)

            # Description
            node_desc = ds_rootnode.find('message:Description', XML_NS).find('message:Indicator', XML_NS)
            ds['full_name'] = node_desc.get('name')
            ds['unit'] = node_desc.find('message:Units', XML_NS).find('message:Unit', XML_NS).get('value')
            nd = node_desc.find('message:Periodicities', XML_NS).find('message:Periodicity', XML_NS)
            ds['periodicity']['value'] = nd.get('value')
            ds['periodicity']['releases'] = nd.get('releases')
            ds['periodicity']['next'] = nd.get('next-release')
            if ds['periodicity']['next']: 
                try:
                    ds['periodicity']['next'] = dt.strptime(ds['periodicity']['next'], '%d.%m.%Y')
                except:
                    pass
            nd = node_desc.find('message:DataRange', XML_NS)
            ds['data_range'] = tuple(int(nd.get(x)) for x in ('start', 'end'))
            ds['updated'] = dt.fromisoformat(node_desc.find('message:LastUpdate', XML_NS).get('value'))
            ds['methodology'] = node_desc.find('message:Methodology', XML_NS).get('value')
            ds['agency_name'] = node_desc.find('message:Organization', XML_NS).get('value')
            ds['agency_dept'] = node_desc.find('message:Department', XML_NS).get('value')
            nd = node_desc.find('message:Allocations', XML_NS).find('message:Allocation', XML_NS)
            ds['classifier']['id'] = nd.get('id')
            ds['classifier']['path'] = nd.find('message:Name', XML_NS).text
            nd = node_desc.find('message:Responsible', XML_NS)
            ds['prepared_by']['name'] = nd.find('message:Name', XML_NS).text
            ds['prepared_by']['contacts'] = nd.find('message:Contacts', XML_NS).text
            ds['data'] = self._get_data(ds_rootnode, ds['codes'])

            if save2json:
                try:
                    with open(os.path.join(self.root_folder, save2json), 'w', encoding='utf-8') as outfile:
                        json.dump(ds, outfile, ensure_ascii=False, indent=4, default=str)
                    print('Saved to JSON')
                except Exception as err:
                    print(err)

        except Exception as err:
            print(err)
            return None

        return ds

    def parse(self, datasets=None, xmlfilenames='auto', overwrite=True, save2json='auto', loadfromjson='auto',
              processes='auto', timeout='auto', on_dataset=None, on_error=None, on_stopcheck=None):

        if not self.datasets: self.update_dataset_list()

        if datasets is None:
            datasets = self.datasets
        elif isinstance(datasets, str):
            datasets = self.find_datasets(datasets)
        elif is_iterable(datasets):
            if len(datasets) == 0:
                print('Empty datasets parameter!')
                return            
            if isinstance(datasets[0], int) or isinstance(datasets[0], str):
                datasets = [self[k] for k in datasets]            
        else:
            print('Bad type: datasets')
            return

        if not datasets:
            print('No datasets matching your request.')
            return

        # prepare args for worker function
        args = []
        for i, ds in enumerate(datasets):
            try:
                if xmlfilenames == 'auto':
                    xmlfilename = ds['identifier'] + '.xml'
                elif is_iterable(xmlfilenames):
                    xmlfilename = xmlfilenames[i]
                else:
                    print('Bad type: xmlfilenames')
                    return
                
                if save2json == 'auto':
                    save2json_ = ds['identifier'] + '.json'
                elif save2json is None:
                    save2json_ = None
                elif is_iterable(save2json):
                    save2json_ = save2json[i]            
                else:
                    print('Bad type: save2json')
                    return

                if loadfromjson == 'auto':
                    loadfromjson_ = ds['identifier'] + '.json'
                elif loadfromjson is None:
                    loadfromjson_ = None
                elif is_iterable(loadfromjson):
                    loadfromjson_ = loadfromjson[i]            
                else:
                    print('Bad type: loadfromjson')
                    return
                
                args.append((ds, xmlfilename, overwrite, save2json_, loadfromjson_))
                
            except Exception as err:
                print(err)
                return

        if processes == 'auto': processes = None
        
        with Pool(processes=processes) as pool:
            results = pool.starmap_async(self.parse_dataset, args, callback=on_dataset, error_callback=on_error)
            pool.close()
            pool.join()