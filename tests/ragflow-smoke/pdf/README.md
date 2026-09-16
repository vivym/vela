# PDF 解析回归样本

这是可核对答案的 4 页合成 PDF，分别覆盖原生文字、表格、双栏与流程图、无文字层扫描页。
生成文件位于 `output/pdf/ragflow-pdf-validation.pdf`，已上传至现有 `qwen3-gpu-smoke`。
当前结果发现一个表格数值丢失和双栏顺序错误；不要修改标准答案来使结果通过。

生成依赖 `reportlab`、`pypdf` 和 Poppler `pdftoppm`，字体需支持中文：

```sh
python3 tests/ragflow-smoke/pdf/build_fixture.py --font /path/to/chinese-font.ttf
```

默认字体路径适用于本次 macOS 环境。构造完成后断言第四页无文字层；渲染 PDF 并检查
四页后再上传。不同字体或依赖版本可能产生不同文件 digest，结果需绑定实际文件。

获取 `GET /api/v1/datasets/{dataset_id}/documents/{document_id}/chunks?page_size=100`
的 JSON 响应，保存到本地后运行：

```sh
python3 tests/ragflow-smoke/pdf/verify_chunks.py chunks.json
```

数字表格以对应行的单元格进行严格匹配，不允许用其他行的 `384` 中的 `4` 充当缺失值。
普通内容检查使用 NFKC/空白归一化。页码来自 chunk 的 positions；跨页 chunk 的事实命中
只代表该片段有该事实，不能证明逐字定位。双栏检查核对编号的完整阅读顺序。
任一内容、表格行或双栏顺序失败时退出 1。

`result-2026-09-16.json` 包含原始解析片段、质量检查和检索结果。复核现有结果：

```sh
python3 - <<'PY'
import importlib.util, json
from pathlib import Path
root = Path('tests/ragflow-smoke/pdf')
spec = importlib.util.spec_from_file_location('verify', root / 'verify_chunks.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
receipt = json.loads((root / 'result-2026-09-16.json').read_text())
expected = json.loads((root / 'expectations.json').read_text())
actual = m.verify({'data': {'chunks': receipt['parsed_chunks']}}, expected)
assert actual == receipt['validation']
assert actual['content_passed'] == 14
assert not actual['column_reading_order_passed']
print('Recorded defects reproduced')
PY
```

检索测试分别记录 `retrieval_hit` 和 `answer_evidence_passed`。检索到表格并不意味着
表格包含所需数值；当前 Cedar-C 问题命中成功、答案证据失败。未配置 Chat 模型，
因此这些结果不代表生成式回答评测。
