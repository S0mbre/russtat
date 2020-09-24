# -*- coding: utf-8 -*-
# --------------------------------------------------------------- #

import xml.etree.ElementTree as ET
import requests
import os
import json
import re

URL_EMISS_LIST = 'https://fedstat.ru/opendata/list.xml'
ROOT_FOLDER = ''

# --------------------------------------------------------------- #

def get_data_list(xmlfilename='list.xml', xml_only=True, overwrite=False, save2json='list_json.json', loadfromjson='list_json.json'):   

    if loadfromjson:
        try:
            with open(os.path.join(ROOT_FOLDER, loadfromjson), 'r', encoding='utf-8') as infile:
                results = json.load(infile)
            print('Loaded from JSON')
            return results
        except Exception as err:
            print(err)
            print('Importing from XML...')
            return get_data_list(xmlfilename, xml_only, overwrite, save2json, None)
        
    outputfile = os.path.join(ROOT_FOLDER, xmlfilename)
    if not os.path.exists(outputfile) or overwrite:       
        try:
            os.remove(outputfile)
            print('Deleted existing XML')
        except:
            pass
        try:
            res = requests.get(URL_EMISS_LIST)
            if not res: return None

            with open(outputfile, 'wb') as outfile:
                outfile.write(res.content)
            print(f'Downloaded XML from {URL_EMISS_LIST}')

        except Exception as err:
            print(err)
            return None

    results = []
    
    tree = ET.parse(outputfile)
    root_el = tree.getroot()

    for item in root_el.find('meta').iter('item'):
        if xml_only and item.find('format').text != 'xml':
            continue
        results.append({child.tag: child.text.strip('"').strip() for child in item})
    print('Loaded from XML')

    if save2json:
        with open(os.path.join(ROOT_FOLDER, save2json), 'w', encoding='utf-8') as outfile:
            json.dump(results, outfile, ensure_ascii=False, indent=4)
        print('Saved to JSON')

    return results

def find_datasets(dslist, pattern, regex=False, case_sense=False, fullmatch=False):
    results = []
    if regex:
        regexp = re.compile(pattern) if case_sense else re.compile(pattern, re.I)

    for item in dslist:
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
    return results

def parse_dataset(dataset, xmlfilename='dataset.xml', overwrite=False, save2json='dataset_json.json', loadfromjson='dataset_json.json'):

    if loadfromjson:
        try:
            with open(os.path.join(ROOT_FOLDER, loadfromjson), 'r', encoding='utf-8') as infile:
                ds = json.load(infile)
            print('Loaded from JSON')
            return ds
        except Exception as err:
            print(err)
            print('Importing from XML...')
            return parse_dataset(dataset, xmlfilename, overwrite, save2json, None)

    if not 'link' in dataset:
        print('Dataset has no "link" object!')
        return None
    if not 'format' in dataset:
        print('Dataset has no "format" object!')
        return None
    if dataset['format'] != 'xml':
        print('Dataset must be in XML format!')
        return None

    outputfile = os.path.join(ROOT_FOLDER, xmlfilename)
    if not os.path.exists(outputfile) or overwrite:       
        try:
            os.remove(outputfile)
            print('Deleted existing XML')
        except:
            pass
        try:
            res = requests.get(dataset['link'])
            if not res: return None

            with open(outputfile, 'wb') as outfile:
                outfile.write(res.content)
            print(f'Downloaded XML from {URL_EMISS_LIST}')

        except Exception as err:
            print(err)
            return None

    ds = None

    tree = ET.parse(outputfile)
    root_el = tree.getroot()

    return ds

def main():
    results = get_data_list()

# --------------------------------------------------------------- #

if __name__ == '__main__':
    main()