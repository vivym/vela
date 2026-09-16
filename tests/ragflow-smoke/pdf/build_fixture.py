#!/usr/bin/env python3
"""Generate a four-page PDF with known answers, including an image-only page."""
import argparse, json, hashlib, subprocess
from pathlib import Path
from reportlab.pdfgen.canvas import Canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import HexColor, white
from reportlab.lib.pagesizes import A4
from pypdf import PdfReader, PdfWriter

p=argparse.ArgumentParser()
p.add_argument('--font',default='/System/Library/Fonts/Supplemental/Arial Unicode.ttf')
p.add_argument('--output',type=Path,default=Path('output/pdf/ragflow-pdf-validation.pdf'))
p.add_argument('--workdir',type=Path,default=Path('tmp/pdfs'))
a=p.parse_args(); a.output.parent.mkdir(parents=True,exist_ok=True); a.workdir.mkdir(parents=True,exist_ok=True)
pdfmetrics.registerFont(TTFont('Chinese',a.font))
W,H=A4
c=Canvas(str(a.workdir/'born-digital.pdf'),pagesize=A4,invariant=1)
c.setTitle('RAGFlow PDF 解析验证 - 合成测试语料')
def text(x,y,s,size=12):
 c.setFillColor(HexColor('#17283b')); c.setFont('Chinese',size); c.drawString(x,y,s)
def page(number,title,sub):
 c.setFillColor(HexColor('#e9f1f7')); c.rect(0,H-150,W,150,fill=1,stroke=0)
 text(45,H-42,'RAGFlow / PDF 解析验收',12)
 text(45,H-85,title,22); text(45,H-118,sub,10)
 text(45,35,'合成测试语料 - 不代表真实业务配置',9); text(W-82,35,f'{number} / 4',10)
page(1,'01 原生文字与中英文混排','检查目标：标题、中文正文、数字、日期和英文标识')
for i,s in enumerate([
 '松鹤测试计划的编号是 SH-2479。',
 '计划负责人是周青，生效日期为 2026-09-16。',
 '本计划要求保留审计记录 17 天。',
 'The document owner is Zhou Qing. Project code: SH-2479.',
 'The audit retention period is 17 days.',
 '',
 '本文是为 PDF 解析验收专门编写的合成资料。',
 '第二页包含设备表格，第三页包含双栏步骤和流程图。',
 '第四页只有一张扫描图片，没有可复制的 PDF 文字层。',
 '扫描页的专用事实不在其他页面重复，需要通过 OCR 获取。',
 ]): text(45,H-195-i*32,s)
c.showPage()
page(2,'02 结构化表格','检查目标：行列对应关系、中文表头、英文设备名和数值')
text(45,H-192,'表 1  测试设备资源清单',14)
rows=[['设备名称','节点数量','内存（GiB）','机房'],['Cedar-A', '12','384','东区'],['Cedar-B','8','256','西区'],['Cedar-C','4','128','南区']]
xs=[45,200,315,440,550]; top=H-224; rh=52
for j,row in enumerate(rows):
 c.setFillColor(HexColor('#dbeaf3') if j==0 else (HexColor('#f4f7fa') if j%2 else white)); c.rect(xs[0],top-(j+1)*rh,xs[-1]-xs[0],rh,fill=1,stroke=0)
 for i,cell in enumerate(row): text(xs[i]+12,top-j*rh-32,cell,12)
c.setStrokeColor(HexColor('#778d9e'))
for x in xs: c.line(x,top,x,top-len(rows)*rh)
for j in range(len(rows)+1): c.line(xs[0],top-j*rh,xs[-1],top-j*rh)
text(45,top-260,'表格注释：内存列为每节点的容量，单位是 GiB。',11)
text(45,top-292,'验收时应保留设备名称与对应行数值之间的关系。',11)
c.showPage()
page(3,'03 双栏阅读顺序与流程图','检查目标：栏内顺序、栏目边界，以及流程标签的保留')
left=['A1 接收申请。','A2 检查文件格式。','A3 校验编号是否完整。','A4 将合格文件提交归档。']
right=['B1 建立审计记录。','B2 记录归档时间。','B3 通知申请人处理结果。','B4 保存本次审核结论。']
text(45,H-190,'左栏：受理步骤',14); text(325,H-190,'右栏：审计步骤',14)
for i in range(4): text(45,H-229-i*40,left[i],11); text(325,H-229-i*40,right[i],11)
c.setStrokeColor(HexColor('#ccd8df')); c.line(290,H-178,290,H-380)
text(45,330,'图 1  归档处理流程',14)
for x,label in [(45,'接收'),(232,'校验'),(419,'归档')]:
 c.setFillColor(HexColor('#e9f1f7')); c.roundRect(x,230,130,60,6,fill=1,stroke=0); text(x+42,254,label,15)
for x in [180,367]:
 c.setStrokeColor(HexColor('#385773')); c.line(x,260,x+42,260); c.line(x+34,266,x+42,260); c.line(x+34,254,x+42,260)
text(45,190,'图注：文件从接收环节进入校验环节，再进入归档环节。',11)
c.showPage(); c.save()
# Create a temporary original page, rasterize it, then embed only the image.
c=Canvas(str(a.workdir/'scan-original.pdf'),pagesize=A4,invariant=1)
page(4,'04 扫描页专用凭证','本页最终以图片嵌入；验证时必须通过 OCR 恢复内容')
for i,s in enumerate(['凭证编号：SCAN-7294','应急联系人：林澄','校验短语：蓝鲸松林','每日巡检时间：06:35','核对金额：1842.75 元','验收说明：以上字段均为虚构测试数据。']):
 text(55,H-215-i*60,s,16 if i<5 else 12)
c.showPage(); c.save()
subprocess.run(['pdftoppm','-f','1','-singlefile','-r','160','-png',str(a.workdir/'scan-original.pdf'),str(a.workdir/'scan-page')],check=True)
c=Canvas(str(a.workdir/'scan-image-only.pdf'),pagesize=A4,invariant=1)
c.drawImage(str(a.workdir/'scan-page.png'),0,0,W,H); c.showPage(); c.save()
w=PdfWriter(); w.append(str(a.workdir/'born-digital.pdf')); w.append(str(a.workdir/'scan-image-only.pdf'))
w.add_metadata({'/Title':'RAGFlow PDF validation: text, table, columns and scanned OCR','/Author':'Vela validation fixture'})
w.write(a.output)
r=PdfReader(a.output); counts=[len(p.extract_text().strip()) for p in r.pages]
assert len(r.pages)==4 and all(counts[i]>100 for i in range(3)) and counts[3]==0,counts
manifest={'file':a.output.name,'sha256':hashlib.sha256(a.output.read_bytes()).hexdigest(),'pages':4,'native_text_chars':counts,'scan_page':4,'scan_has_text_layer':False}
(a.output.with_suffix('.manifest.json')).write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(manifest,ensure_ascii=False))
