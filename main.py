
from multiprocessing.pool import Pool
import requests
from tqdm import tqdm
import os
from lm_dataformat import Archive
import shutil
import spacy
import json
import glob
import fitz #pip install PyMuPDF
import xml.etree.ElementTree as ET
import urllib.robotparser


def query_bn_repos(repos: list = ['books', 'chapters', 'articles']):
    
    """This method queries bibiliotekanauki.pl repositories via api and returns generator that yields 
     file url and document title. Developed based on https://doc.bibliotekanauki.pl/pl/oai-pmh-api-documentation
     Querying arcticles repo browses tens of thousands of pages and is time consuming. Therefore generator is used
     to allow consuming returned data asap.
     
     Parameters
     ----------
     repos : list, optional
     Repository names to be queried 
     Valid values 'books', 'articles', 'chapters' (default all)
     """
   
    ns = {
        'default': 'http://www.openarchives.org/OAI/2.0/',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'datacite': 'http://datacite.org/schema/kernel-4',
        'oaire': 'http://namespace.openaire.eu/schema/oaire/',
        'xml': 'http://www.w3.org/XML/1998/namespace'
        }


    for repo in repos:

        url = 'https://bibliotekanauki.pl/api/oai/'+repo+'?verb=ListRecords'
        params = '&metadataPrefix=oai_openaire'
        
        while params:

            #if not rp.can_fetch('*', url+params):
            #    print("Cannot access the website")
             #   params = ''
             #   continue

            resp = requests.get(url+params)
     
            if resp.ok:

                root = ET.fromstring(resp.content)
                resumption_token = root.find("./default:ListRecords/default:resumptionToken", namespaces=ns).text
                
                #If resumption token is returned additional data exists
                if resumption_token:
                        params = '&resumptionToken='+resumption_token
                                        
                else:
                        params = '' 


                for record in root.findall("./default:ListRecords/default:record", namespaces=ns):
                        #Check if document language is polish
                        if record.find('./default:metadata/oaire:resource/dc:language', namespaces=ns) != None and record.find('./default:metadata/oaire:resource/dc:language', namespaces=ns).text=='pol':
                            #Check if pdf format is available
                            if record.find("./default:metadata/oaire:resource/oaire:file[@mimeType='application/pdf']", namespaces=ns) != None:
                                file = record.find("./default:metadata/oaire:resource/oaire:file[@mimeType='application/pdf']", namespaces=ns).text
                                #Check if polish title is present if not the first title is used.
                                if record.find("./default:metadata/oaire:resource/datacite:titles/datacite:title[@xml:lang='pl']", namespaces=ns) != None:
                                    title = record.find("./default:metadata/oaire:resource/datacite:titles/datacite:title[@xml:lang='pl']", namespaces=ns).text
                                else:
                                    title = record.find("./default:metadata/oaire:resource/datacite:titles/datacite:title", namespaces=ns).text
                                yield file, title   
            else:
                params = ''    
    return               


def get_pdf_text(file: str):
    text = ""
    try:
        with fitz.open(file) as pdf:
    
            for page in pdf:
                try:
                    text += page.get_text()
                except Exception as e:
                    print("Page reading problem: "+ type(e))
    except:
        print("Error opening pdf file: "+ type(e))     

    return text


def download_and_read_pdf(url: str):

    ok = True
    file_name = url.split('/')[-1]
    txt = ''
    if  rp.can_fetch('*', url):
        
        try:
            response = requests.get(url, stream=True)
            total_size_in_bytes = int(response.headers.get('content-length', 0))
            block_size = 1024
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
            with open(file_name, 'wb') as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)
            progress_bar.close()
            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                ok = False
        except:
            print("Error downloading {0}".format(url))
            ok = False

        if ok:
            txt = get_pdf_text(file_name)

        #If no text layer in pdf found short or 0-length text is returned.    
        if len(txt)<100:
            ok = False
        try:
            os.remove(file_name)    
        except:
            print("Error deleting file {0}".format(file_name))
    else:
        print("Cannot access the file")
        ok = False

    return ok, txt


def get_word_stats(txt):
    if not txt:
        return 0, 0, 0, 0, 0, 0

    sentences = 0
    words = 0
    verbs = 0
    nouns = 0
    punctuations = 0
    symbols = 0

    doc = nlp(txt)

    sentences = len(list(doc.sents))
    words = len([token.text for token in doc if not token.is_punct])
    nouns = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "NOUN")])
    verbs = len([token.text for token in doc if (not token.is_stop and not token.is_punct and token.pos_ == "VERB")])
    punctuations = len([token.text for token in doc if (token.is_punct or token.pos_ == "PUNCT")])
    symbols = len([token.text for token in doc if (token.pos_ == "SYM")])

    return sentences, words, verbs, nouns, punctuations, symbols

def process_item(book_info):

    title = book_info[1]
    file_url = book_info[0]
    meta = {}
    print(title)
    ok, txt = download_and_read_pdf(file_url)
    if ok:
        l = len(txt.strip())
        if l > 100000:
            nlp.max_length = len(txt) + 100
        sentences, words, verbs, nouns, punctuations, symbols = get_word_stats(txt.strip())
        meta = {'url' : file_url, 'title': title, 'length': l, 'sentences': sentences, 'words': words, 'verbs': verbs, 'nouns': nouns, 'punctuations': punctuations, 'symbols': symbols}
    return ok, txt.strip(), meta

def initialize_worker():

    print('Initializing worker...')   

    #Each worker node needs to have its own resources.
    global rp
    global nlp
   
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url('https://bibliotekanauki.pl/robots.txt')
    rp.read()


    #Disabling some unused model features speeds things up to 20%
    nlp = spacy.load("pl_core_news_md", disable=('ner','lemmatizer','textcat','entity_linker'))
      


if __name__ == '__main__':


    ar = Archive('./data')

    file_name_zst = './biblioteka_nauki_pl_corpus.jsonl.zst'
    file_name_manifest = './biblioteka_nauki_pl_corpus.manifest'

    total_len = 0
    total_docs = 0
    total_sentences = 0
    total_words = 0
    total_verbs = 0
    total_nouns = 0
    total_punctuations = 0
    total_symbols = 0

    # create and configure the process pool. All available resources are used by default
    
    with Pool(initializer=initialize_worker) as pool:
        # issue tasks to the process pool
        for ok, txt, meta in pool.imap(process_item, query_bn_repos()):
            if ok:

                total_words += meta['words']
                total_verbs += meta['verbs']
                total_nouns += meta['nouns']
                total_len += meta['length']
                total_docs += 1
                total_sentences += meta['sentences']
                total_punctuations += meta['punctuations']
                total_symbols += meta['symbols']
                ar.add_data(txt, meta = meta)
                print("Added " + meta.get('url'))
        # Close the process pool
        pool.close()
        # Wait for all tasks to complete
        pool.join()
    ar.commit()


    data_files= glob.glob('./data/*')
    file_size = 0

    #This solves an issue where data_files remains locked after ar commiting, causing error on cleanup
    ar = None

    for f in data_files:
        if f.endswith('.zst'):
            shutil.copy(f, os.path.join(file_name_zst))
            file_size = os.path.getsize(file_name_zst)

        os.remove(f)

    manifest = {"project" : "SpeakLeash", "name": "biblioteka_nauki_pl_corpus", "description": "Collection of Polish science books, chapters and articles from bibliotekanauki.pl corpus", "license": "TBC", "language": "pl", "file_size" : file_size, "sources": [{"name": "biblioteka_nauki_pl_corpus", "url": "https://bibliotekanauki.pl", "license": "TBC"}], "stats": {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len, "punctuations" : total_punctuations, "symbols" : total_symbols}}
    json_manifest = json.dumps(manifest, indent = 4) 

    with open(file_name_manifest, 'w') as mf:
        mf.write(json_manifest)