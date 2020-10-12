# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.rsengine
# @brief EMISS data retrieving and processing engine.
import xml.etree.ElementTree as ET
import requests, os, sys, json, re
from datetime import datetime as dt, timedelta
from dask.distributed import Client as DaskClient, Variable as DaskVariable, as_completed
from globs import *

## `str` permanent URL of the EMISS dataset list
URL_EMISS_LIST = 'https://fedstat.ru/opendata/list.xml'
## `dict` EMISS XML dataset schemas
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

## @brief EMISS data retrieving and processing engine. 
# Downloads source XML's, parses them into Python dictionaries and passes
# on to a custom callback function. Datasets can optionally to loaded from
# and saved as JSON files, to maximize portability. Batch processing is
# enhanced by multiprocessing, to handle datasets in parallel taking advantage
# of your CPU's multiple cores.
class Russtat:

    ## @param root_folder `str` path to the data directory where the source XML
    # files and JSON files will be saved / searched (relative to project dir or absolute).
    # Default is empty string, which stands for the project root directory.
    # @param update_list `bool` whether the list of datasets ('list_json.json')
    # must be refreshed from the server (default = `False`). If 'list_json.json'
    # is absent from `root_folder`, it will be downloaded in any case.
    # @param connection_timeout `float` | `2-tuple` timeout in seconds
    # for server connection / data reception. If it's a single value (`float`),
    # it will be used for both connection and data timeout; if it's a `tuple`,
    # the first value stands for connection, the second for data timeout.
    def __init__(self, root_folder='', update_list=False, connection_timeout=10, processes=None):    
        ## `str` path to the root data directory for XML / JSON files
        self.root_folder = root_folder
        ## `list` list of available statistical datasets on the EMISS server
        self.datasets = []
        ## `iterator` iterator for Russtat::datasets
        self._iter = None
        ## `Dask Client`
        self.cluster = DaskClient(n_workers=processes, threads_per_worker=1)
        ## `Dask Variable` stop flag for Dask cluster -- see [Dask docs](https://docs.dask.org/en/latest/futures.html#global-variables)
        self._stopped = DaskVariable('stopcheck', client=self.cluster)
        self._stopped.set(False)
        ## `float` | `2-tuple` timeout in seconds for server connection / data reception
        self.connection_timeout = connection_timeout
        self.update_dataset_list(overwrite=update_list, loadfromjson='list_json.json' if not update_list else '')

    
    def __del__(self):
        if self.cluster:
            self.cluster.close()

    ## Iterator method to iterate the available remote datasets, e.g.
    # ```
    # for dataset in Russtat():
    #     print(dataset['title'])
    # ```
    # @returns Russtat::_iter
    def __iter__(self):
        self._iter = self.datasets.__iter__()
        return self._iter

    ## Next iterator in the available remote datasets.
    # @see \_\_iter\_\_()
    def next(self):
        if not self.datasets:
            raise StopIteration
        return next(self._iter)

    ## len() function overload.
    # @returns `int` number of available EMISS datasets on the server
    def __len__(self):
        return len(self.datasets)

    ## bool() type cast operator overload.
    # @returns `bool` `True` if there is at least one EMISS dataset available.
    def __bool__(self):
        return bool(self.__len__())

    ## Indexing operator [] overload.
    # @param `int` | `str` | `slice` dataset index: numerical 
    # (position of dataset in Russtat::datasets) or string (full title of the dataset)
    # @returns `dict` | `list of dict` | `None` found dataset(s) from Russtat::datasets 
    # or `None` if nothing is found 
    # <br><br><b>EXAMPLES:</b><br>
    # ```
    # engine = Russtat()
    #
    # # get first dataset
    # ds = engine[0]
    #
    # # get first 10 datasets
    # dsets = engine[:10]
    #
    # # get last 100 datasets
    # dsets = engine[-100:]
    #
    # # get dataset with title 'Количество дел, принятых  к производству следователями'
    # ds = engine['Количество дел, принятых  к производству следователями']
    # ``` 
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

    ## Updates the dataset list from the server, downloading and parsing rsengine::URL_EMISS_LIST.
    # @param xmlfilename `str` filename to store the downloaded XML file 
    # (relative to Russtat::root_folder or absolute)
    # @param xml_only `bool` allows to check that the downloaded file is in XML format:
    # if `True` (default), other file formats will not be accepted and downloaded
    # @param overwrite `bool` set to `True` to force update of the existing XML in the 
    # target directory; if `False` (default), the existing XML will not be overwritten
    # @param del_xml `bool` set tot `True` (default) to delete the XML file after
    # downloading and parsing into Russtat::datasets; `False` to keep the XML file
    # @param save2json `str` | `None` | `bool` if a string is passed, it must be
    # the file name of the JSON file to save the parsed dataset list to
    # (default = 'list_json.json'). If `None` or `False`, JSON is not saved.
    # @param loadfromjson `str` | `None` | `bool` if a string is passed, it must be
    # the file name of the JSON file to load the dataset list from
    # (default = 'list_json.json'). If a valid file is found, the list will not be
    # downloaded / parsed from XML, but loaded directly from JSON. If `None` or `False`, 
    # the dataset list will be instead retrieved from the server or stored XML file 
    # (see `xmlfilename` parameter).
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

    ## Searches for datasets in their 'title' field for a given pattern.
    # @param pattern `str` substring / pattern to search for
    # @param regex `bool` whether the pattern is a simple substring or a regular expression
    # @param case_sense `bool` whether the search must be case-sensitive or not
    # @param fullmatch `bool` whether the search must match the entire pattern
    # @returns `list` list of dataset objects found
    # <br><br><b>EXAMPLES:</b><br>
    # ```
    # engine = Russtat()
    # 
    # # simple search: all datasets containing 'документов'
    # res = engine.find_datasets('документов')
    # 
    # # regex search: all datasets containing 'женщ' or 'мужч'
    # res = engine.find_datasets('женщ|мужч', True)
    # 
    # # more complex regex search
    # res = engine.find_datasets(r'([\w\s,\-]*)(техн)([\w\s,\-]*)(отход)', True)
    # ```
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

    ## Prints a message to a file stream / console accounting for globs::DEBUGGING flag.
    # @param message `str` message to output
    # @param force_print `bool` set to `True` to output message disregarding globs::DEBUGGING
    # @param file `file` file stream to output the message to (default = STDOUT)
    # @param end `str` message ending suffix (default = new line symbol)
    # @param flush `bool` `True` to flush the IO buffer immediately
    def _report(self, message, force_print=False, file=sys.stdout, end='\n', flush=False):
        if force_print or DEBUGGING:
            print(message, end=end, file=file, flush=flush)

    ## Gets the value (text) of a given XML node / children with an optional default value.
    # @param node `ElementTree node` the parent XML node
    # @param tags `list` | `str` | `None` a single child tag or list of child tags in
    # hierarchical order (`[child, sub-child, sub-sub-child, ...]`) to get the target node.
    # If `None`, the parent node is the target one.
    # @param default `str` default value returned in case of failure
    # @param ns `dict` XML namespace schema (default = rsengine::XML_NS)
    # @param strip `bool` whether to apply `strip()` to the retrieved text value
    # to get rid of leading / trailing spaces
    # @returns `str` found value / `default` on failure
    # @see [Python ElementTree API](https://docs.python.org/3.8/library/xml.etree.elementtree.html)
    # @see \_get_attr()
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
    
    ## Gets the value (text) of a given attribute of an XML node / its children 
    # with an optional default value.
    # @param node `ElementTree node` the parent XML node
    # @param attr `str` the attribute name (key)
    # @param tags `list` | `str` | `None` a single child tag or list of child tags in
    # hierarchical order (`[child, sub-child, sub-sub-child, ...]`) to get the target node.
    # If `None`, the parent node is the target one.
    # @param default `str` default value returned in case of failure
    # @param ns `dict` XML namespace schema (default = rsengine::XML_NS)
    # @param strip `bool` whether to apply `strip()` to the retrieved text value
    # to get rid of leading / trailing spaces
    # @returns `str` found value / `default` on failure
    # @see [Python ElementTree API](https://docs.python.org/3.8/library/xml.etree.elementtree.html)
    # @see \_get_text()
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

    ## Parses and collects the CodeLists section of a dataset XML.
    # @param ds_rootnode `ElementTree node` the parent node containing 'CodeLists'
    # @returns `dict` CodeLists section converted into a dictionary in the format:<br>
    # ```
    # {
    #  'code-name': {'name': '<full name>', 'values': [('<id>', '<description>'), ...]},
    #  'code-name': {...}
    # }
    # ```
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

    ## Parses and collects the DataSet section of a dataset XML.
    # @param ds_rootnode `ElementTree node` the parent node containing 'DataSet'
    # @param codes `dict` CodeLists section as a dictionary -- see \_get_codes()
    # @param max_row `int` limit of data points to collect from the XML
    # (`-1` = no limit)
    # @returns `list` DataSet section as a list of data values in the format:<br>
    # ```
    # [('classifier', 'class', 'unit', 'period of observation', int('observation year'), float('observation value')), ...]
    # ```
    def _get_data(self, ds_rootnode, codes, max_row=-1):
        n = 0
        dataset = ds_rootnode.find('message:DataSet', XML_NS)
        if not dataset:             
            return []
        data = []
        for item in dataset.iterfind('generic:Series', XML_NS):
            
            try:

                # period and unit
                per, ei = ('', '')
                try:
                    for attr in item.find('generic:Attributes', XML_NS).iterfind('generic:Value', XML_NS):
                        concept = self._get_attr(attr, 'concept')
                        val = self._get_attr(attr, 'value')
                        if concept == 'EI':
                            ei = val
                        elif concept == 'PERIOD':
                            per = val
                except:
                    per, ei = ('', '')

                # year
                try:
                    tim = int(self._get_text(item, ['generic:Obs', 'generic:Time'], '0'))
                except:
                    tim = 0

                # value
                try:
                    val = float(self._get_attr(item, 'value', ['generic:Obs', 'generic:ObsValue'], '0.0').replace(',', '.').replace(' ', ''))
                except:
                    val = 0.0

                # classifier and class
                try:
                    for key_item in item.find('generic:SeriesKey', XML_NS).iterfind('generic:Value', XML_NS):                
                    
                        key_concept = self._get_attr(key_item, 'concept')
                        key_key = self._get_attr(key_item, 'value')
                        classifier, cl = ('', '')
                        
                        for code in codes:
                            if code == key_concept:
                                classifier = codes[code]['name']
                                for cval in codes[code]['values']:
                                    if cval[0] == key_key:
                                        cl = cval[1]
                                        break
                                break
                        
                        data.append((classifier, cl, ei, per, tim, val))
                        n += 1
                        if max_row > 0 and n > max_row: break
                        
                except:
                    data.append(('', '', ei, per, tim, val))
                    n += 1
                    if max_row > 0 and n > max_row: break

            except Exception as err:
                self._report(err)
                break

        return data

    ## Retrieves and parses a single EMISS dataset from an XML/JSON file,
    # optionally saving it as JSON and passing into a custom callback.
    # @param dataset `dict` | `int` | `str` the source dataset to parse or find
    # in the available datasets list.<br>
    # If it's a dictionary, it must have the following keys:<br>
    # ```
    # {
    #    "identifier": "<dataset ID, coincides with source XML file name>",
    #    "title": "<dataset title>",
    #    "link": "<full URL to source XML at https://fedstat.ru/opendata/>",
    #    "format": "xml" #          <-- ONLY XML ACCEPTED!
    # }
    # ```
    # If it's an integer, it must be the index of a dataset in Russtat::datasets.<br>
    # If it's a string, it means part of the dataset title to find using simple search
    # -- see find_datasets().
    # @param xmlfilename `str` output file name for the downloaded XML; if 'auto' is
    # passed, the dataset 'identifier' key will be used to generate the file name.
    # @param overwrite `bool` whether to overwrite the existing XML file on name conflict
    # @param del_xml `bool` set to `True` to delete the XML file after successful operation
    # @param save2json `str` | `None` | `bool` if a string is passed, it must be
    # the file name of the JSON file to save the parsed data or 'auto' to generate
    # the file name automatically from the XML file name. Otherwise, `None` or `False`
    # means that no JSON file will be created.
    # @param loadfromjson `str` | `None` | `bool` same as `save2json`, but for loading
    # the dataset. If it's a non-null string ('auto' or file name), the dataset will
    # be loaded directly from that JSON file, without downloading and parsing the XML.
    # @param on_dataset `callback` | `None` callback function to pass the parsed dataset object (dictionary).
    # It must take at least the dataset as its first (positional) parameter and 
    # may also have arbitrary keyword parameters (see `on_dataset_kwargs`).
    # @param on_dataset_kwargs `dict` | `None` optional keyword parameters passed to the
    # callback function (`on_dataset`); if `None`, no such parameters are passed
    # @returns `dict` | `None` the parsed dataset as a dictionary with these keys:<br>
    # ```
    # {
    #   'prepared': None,   # datetime: date/time when the dataset was retrieved from EMISS, e.g. 2020-10-07 03:13:25
    #   'id': '',           # int: dataset ID, e.g. '6804027'
    #   'agency_id': '',    # int: agency ID, e.g. '48'
    #   'codes': {},        # dict: codes, e.g. {'OKSM': {'name': 'ОКСМ', 'values': [('643', 'Российская Федерация')]}}
    #   'full_name': '',    # str: full dataset name, e.g. 'Личные переводы в процентном отношении к валовому внутреннему продукту'
    #   'unit': '',         # str: unit of measurement, e.g. 'процент'
    #   'periodicity': {    # dict: dataset periodicity
    #       'value': '',    # str: periodicty description, e.g. 'Квартальная - за отчетный квартал'
    #       'releases': '', # str: this release period, e.g. '30 апреля'
    #       'next': None    # datetime: next release date/time, e.g. 2021-04-30 00:00:00
    #   }, 
    #   'data_range': (-1, -1),     # tuple of int: start and end years of the observations, e.g. (2014, 2019)
    #   'updated': None,    # datetime: date/time when the dataset was last updated, e.g. 2020-08-27 17:08:38
    #   'methodology': '',  # str: dataset methodology description, e.g. 'Личные переводы в процентном отношении к валовому внутреннему продукту'
    #   'agency_name': '',  # str: preparing agency, e.g. 'Центральный банк Российской Федерации'
    #   'agency_dept': '',  # str: agency responsible department, e.g. 'Департамент статистики'
    #   'classifier': {     # dict: dataset thematic classification
    #       'id': '',       # str: internal classifier number, e.g. '2.8'
    #       'path': ''      # str: classifier path/name, e.g. 'По федеральному плану ... / ... / Показатели ... развития Российской Федерации'
    #   }, 
    #   'prepared_by': {    # dict: who the dataset was prepared by    
    #       'name': '',     # str: name of responsible person
    #       'contacts': ''  # str: contact e-mail / phones etc.
    #   }, 
    #   'data': []          # observation data, see _get_data()
    # }
    # ```
    # In case of failure, `None` is returned.
    # @warning If you plan to read data regularly from your datasets, I recommend setting
    # `save2json` and `loadfromjson` to 'auto' to ensure that datasets are downloaded and
    # parsed from the source XML files <b>only once</b> and then just loaded from the 
    # stored JSON files in your local directory. This will significantly increase the 
    # data processing speed, since no remote requests / downloads or heavy-weight XML
    # parsing will be needed once you've got them downloaded initially.
    # @see get_many()
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

        ds = {'prepared': dt.now(), 'id': dataset['identifier'], 'agency_id': '', 'codes': {}, 
              'full_name': dataset['title'], 'unit': '', 'periodicity': {'value': '', 'releases': '', 'next': dt.fromisoformat('1900-01-01')}, 
              'data_range': (-1, -1), 'updated': dt.fromisoformat('1900-01-01'), 'methodology': '', 'agency_name': '', 'agency_dept': '', 
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
            ds['full_name'] = ' '.join(self._get_attr(node_desc, 'name').split())
            ds['unit'] = self._get_attr(node_desc, 'value', ['message:Units', 'message:Unit'])
            ds['periodicity']['value'] = self._get_attr(node_desc, 'value', ['message:Periodicities', 'message:Periodicity'])
            ds['periodicity']['releases'] = self._get_attr(node_desc, 'releases', ['message:Periodicities', 'message:Periodicity'])
            ds['periodicity']['next'] = dt.strptime(self._get_attr(node_desc, 'next-release', ['message:Periodicities', 'message:Periodicity'], '01.01.1900'), '%d.%m.%Y') - timedelta(hours=3)
            ds['data_range'] = tuple(int(self._get_attr(node_desc, x, 'message:DataRange', '0')) for x in ('start', 'end'))
            ds['updated'] = dt.fromisoformat(self._get_attr(node_desc, 'value', 'message:LastUpdate', '1900-01-01')) - timedelta(hours=3)
            ds['methodology'] = ' '.join(self._get_attr(node_desc, 'value', 'message:Methodology').split())
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
            
            # try to process empty dataset
            if on_dataset: 
                try:
                    if on_dataset_kwargs:
                        on_dataset(ds, **on_dataset_kwargs)
                    else:
                        on_dataset(ds)
                except:
                    pass

            if del_xml:
                try:
                    os.remove(outputfile)
                    self._report(f'Deleted XML ({outputfile})')
                except Exception as err2:
                    self._report(err2)
            return None

        return ds

    ## @brief Retrieves and parses multiple EMISS datasets from XML/JSON files,
    # optionally saving them as JSON and passing into a custom callback.
    # This is the batch version of get_one().
    # @param datasets `iterable` | `str` | `int` | `None` the source datasets to find and parse.
    # May be passed as a list or any iterable of dataset dictionaries, string or integer
    # indices, or a single integer / string index (see `dataset` parameter in get_one()). 
    # If `None` is passed, <b>ALL</b> the available datasets in Russtat::datasets will
    # be processed (which may not be what you need, so check out!). The default value is 0,
    # i.e. only the first dataset in Russtat::datasets.<br> 
    # Also, if `loadfromjson` is a list of file names or a single file name (not 'auto') 
    # and `datasets` is `None`, the datasets will be loaded directly from the indicated JSON files.
    # @param xmlfilenames `str` | `iterable` output file name(s) for the downloaded XMLs --
    # see `xmlfilename` parameter in get_one() for details
    # @param overwrite `bool` whether to overwrite the existing XML files on name conflict
    # @param del_xml `bool` set to `True` to delete the XML file after successful operation
    # @param save2json `str` | `iterable` output file name(s) for JSON files --
    # see `save2json` parameter in get_one() for details
    # @param loadfromjson `str` | `iterable` input file name(s) to load datasets from JSON files --
    # see `loadfromjson` parameter in get_one() for details
    # @param processes `int` | `str` number of processes to parallelize the operation;
    # set to 'auto' to use a default value (auto-calculated by the number of CPU cores)
    # @param on_dataset `callback` | `None` callback function to pass the parsed dataset object --
    # see `on_dataset` parameter in get_one() for details
    # @param on_dataset_kwargs `dict` | `None` optional keyword parameters passed to the
    # callback function (`on_dataset`) -- see `on_dataset_kwargs` parameter in get_one() for details
    # @returns `list` list of datasets (`dict` objects)
    def get_many(self, datasets=0, xmlfilenames='auto', overwrite=True, del_xml=True, 
              save2json='auto', loadfromjson='auto',
              on_dataset=None, on_dataset_kwargs=None):

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
                    
                    args.append((ds, xmlfilename, overwrite, del_xml, save2json_, loadfromjson_, None, None))
                    
                except Exception as err:
                    self._report(err, True)
                    return None       

        #self.cluster.restart()
        self.set_stopped(False)

        futures = self.cluster.map(self.get_one, *args, pure=False)
        results = []

        try:            
            #results = client.gather(futures)
            #client.close()
            for _, result in as_completed(futures, with_results=True):
                results.append(result)
                if self._stopped.get():
                    self.cluster.cancel(futures, force=False)
                    return results
            
            return results

        except Exception as err:
            self.cluster.cancel(futures, force=False)            
            self._report(err, True)
            return None   

    def is_stopped(self):
        return self._stopped.get()

    def set_stopped(self, st=True):
        self._stopped.set(st)

    def filter_datasets_only_new(self, db, datasets=None):
        if datasets is None: datasets = self.datasets
        datasets = set(ds['title'] for ds in datasets)
        res = db.fetch('select distinct name from datasets')
        existing_ds = set(t[0] for t in res) if res else set()
        return list(datasets - existing_ds)

    def filter_datasets_only_existing(self, db, datasets=None):
        if datasets is None: datasets = self.datasets
        datasets = set(ds['title'] for ds in datasets)
        res = db.fetch('select distinct name from datasets')
        existing_ds = set(t[0] for t in res) if res else set()
        return list(datasets & existing_ds)