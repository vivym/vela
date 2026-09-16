#!/usr/bin/env python3
"""Check known PDF facts in parsed chunks, without treating task DONE as quality."""
import argparse,json,re,unicodedata
from html.parser import HTMLParser
from pathlib import Path

def norm(s):
    return re.sub(r'\s+','',unicodedata.normalize('NFKC',s))

class Rows(HTMLParser):
    def __init__(self):
        super().__init__(); self.rows=[]; self.current=None; self.cell=None
    def handle_starttag(self,tag,attrs):
        if tag=='tr': self.current=[]
        if tag in ('td','th'): self.cell=[]
    def handle_data(self,data):
        if self.cell is not None: self.cell.append(data)
    def handle_endtag(self,tag):
        if tag in ('td','th') and self.current is not None and self.cell is not None:
            self.current.append(''.join(self.cell)); self.cell=None
        if tag=='tr' and self.current is not None:
            self.rows.append(self.current); self.current=None

def verify(payload,expected):
    chunks=payload['data']['chunks']
    table_rows=[]
    for chunk in chunks:
        parser=Rows(); parser.feed(chunk['content']); table_rows.extend(parser.rows)
    checks=[]
    for check in expected['checks']:
        relevant=[c for c in chunks if check['page'] in [int(p[0]) for p in c.get('positions',[])]]
        content=norm('\n'.join(c['content'] for c in relevant))
        missing=[t for t in check['tokens'] if norm(t) not in content]
        if check['name'].startswith('table_row_'):
            rows=[row for row in table_rows if row and norm(row[0])==norm(check['tokens'][0])]
            cells=[norm(cell) for row in rows for cell in row]
            missing=[t for t in check['tokens'] if norm(t) not in cells]
        checks.append({'name':check['name'],'page':check['page'],'passed':not missing,'missing':missing})
    table_checks=[]
    for check in expected['checks']:
        if check['name'].startswith('table_row_'):
            matches=[row for row in table_rows if [norm(cell) for cell in row]==[norm(t) for t in check['tokens']]]
            table_checks.append({'name':check['name'],'html_row_preserved':bool(matches),'matching_rows':matches})
    page3=[c for c in chunks if 3 in [int(p[0]) for p in c.get('positions',[])]]
    order=re.findall(r'\b[AB][1-4]\b','\n'.join(c['content'] for c in page3))
    return {'chunks':len(chunks),'pages_observed':sorted({int(p[0]) for c in chunks for p in c.get('positions',[])}),
            'content_checks':checks,'content_passed':sum(c['passed'] for c in checks),'content_total':len(checks),
            'table_row_checks':table_checks,'column_marker_sequence':order,
            'column_reading_order_passed':order==['A1','A2','A3','A4','B1','B2','B3','B4']}

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('chunks',type=Path); p.add_argument('--expected',type=Path,default=Path(__file__).with_name('expectations.json'))
    a=p.parse_args(); result=verify(json.loads(a.chunks.read_text()),json.loads(a.expected.read_text()))
    print(json.dumps(result,ensure_ascii=False,indent=2))
    raise SystemExit(0 if result['content_passed']==result['content_total'] and result['column_reading_order_passed'] and all(r['html_row_preserved'] for r in result['table_row_checks']) else 1)
