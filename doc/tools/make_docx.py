"""Turn the docs' .md files into Word files.

Usage:
  py doc/tools/make_docx.py <doc.md> <output.docx>     one file
  py doc/tools/make_docx.py --all <output folder>      every doc, in the
                                                       same folders as doc/

Reads the simple Markdown the docs use: "# " title, "## " and "### "
sections, "N. " numbered steps, "- " bullets, **bold**, ``` blocks
and plain paragraphs. The look: Calibri Light, black headings,
numbered steps, ASCII tables as real Word tables, other code blocks
in gray Consolas boxes (smaller type for wide ones, so rows don't
wrap), US Letter with 1-inch margins. Standard library only (no
python-docx needed).
"""
import re
import sys
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

GRAY = 'E7E6E6'
W = ('xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
     'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"')



def clean(s):
    return s.replace('`', '').replace('\\<', '<').replace('\\*', '*')


def runs(text, size=None):
    rpr_size = f'<w:sz w:val="{size}"/><w:szCs w:val="{size}"/>' if size else ''
    out = []
    for k, part in enumerate(re.split(r'\*\*(.+?)\*\*', text)):
        if part:
            rpr = ('<w:b/>' if k % 2 else '') + rpr_size
            rpr = f'<w:rPr>{rpr}</w:rPr>' if rpr else ''
            out.append(f'<w:r>{rpr}<w:t xml:space="preserve">{escape(part)}</w:t></w:r>')
    return ''.join(out)


def para(style, text='', num=None, size=None):
    ppr = f'<w:pStyle w:val="{style}"/>'
    if num is not None:
        ppr += f'<w:numPr><w:ilvl w:val="0"/><w:numId w:val="{num}"/></w:numPr>'
    return f'<w:p><w:pPr>{ppr}</w:pPr>{runs(text, size) if text else ""}</w:p>'


def table_rows(lines):
    """Rows of an ASCII table, as (widths, rows), or None. Each row is a list
    of (text, span) cells; span is how many columns the cell covers.

    Grid tables: +---+ borders, | cells; a cell's wrapped lines are joined.
    A line missing an inner | merges the cells on either side, e.g. a group
    heading row across the whole table.
    Dashed tables: a header line, then a line of ---- runs under each column.
    """
    if len(lines) >= 3 and lines[0].startswith('+-') and all(l[:1] in '+|' for l in lines):
        cuts = [i for i, ch in enumerate(lines[0]) if ch == '+']
        rows, cur = [], None

        def flush():
            spans, parts = cur
            rows.append([(' '.join(x for x in p if x), s) for p, s in zip(parts, spans)])

        for l in lines[1:]:
            if l.startswith('+'):
                if cur:
                    flush()
                cur = None
                continue
            bounds = [c for c in cuts if c < len(l) and l[c] == '|']
            spans = [sum(1 for c in cuts if a < c <= b) for a, b in zip(bounds, bounds[1:])]
            cells = [l[a + 1:b].strip() for a, b in zip(bounds, bounds[1:])]
            # A line with every cell filled starts a new row (tables without
            # dividers between rows); one with an empty cell is a wrapped line.
            if cur and (cur[0] != spans or all(cells)):
                flush()
                cur = None
            cur = cur or (spans, [[] for _ in cells])
            for c, text in zip(cur[1], cells):
                c.append(text)
        widths = [b - a for a, b in zip(cuts, cuts[1:])]
        return widths, rows
    if len(lines) >= 3 and re.fullmatch(r'-+(?: +-+)+', lines[1].strip()):
        runs = [m.span() for m in re.finditer(r'-+', lines[1])]
        starts = [a for a, _ in runs] + [None]
        cut = lambda l: [(l[a:b].strip(), 1) for a, b in zip(starts, starts[1:])]
        rows = [cut(lines[0])] + [cut(l) for l in lines[2:] if l.strip()]
        widths = [max(len(r[j][0]) for r in rows) + 2 for j in range(len(runs))]
        return widths, rows
    return None


def word_table(parsed, indented):
    # Like the BOBJ doc's tables: full width, thin black borders, the table
    # shaded blue-gray, a bold header row.
    widths, rows = parsed
    # Indented by the cell padding, so cell text lines up with body text.
    total = 9360 - (360 if indented else 0) - 100
    # Each column at least fits its longest word (about 95 twips a
    # character at 10 pt, plus cell padding); the rest is shared in
    # proportion to the ASCII widths.
    words = [[''] for _ in widths]
    for r in rows:
        if all(span == 1 for _, span in r):
            for j, (text, _) in enumerate(r):
                words[j] += text.split()
    need = [max(len(w) for w in col) * 95 + 220 for col in words]
    if sum(need) <= total:
        spare = total - sum(need)
        grid = [n + round(spare * w / sum(widths)) for n, w in zip(need, widths)]
    else:
        grid = [max(600, round(total * w / sum(widths))) for w in widths]
    grid[-1] += total - sum(grid)
    border = ''.join(f'<w:{s} w:val="single" w:sz="4" w:space="0" w:color="000000"/>'
                     for s in ('top', 'left', 'bottom', 'right', 'insideH', 'insideV'))
    out = [f'<w:tbl><w:tblPr><w:tblW w:w="{total}" w:type="dxa"/>'
           f'<w:tblInd w:w="{(360 if indented else 0) + 100}" w:type="dxa"/>'
           f'<w:tblBorders>{border}</w:tblBorders>'
           '<w:tblLayout w:type="fixed"/>'
           '<w:tblCellMar><w:left w:w="100" w:type="dxa"/><w:right w:w="100" w:type="dxa"/></w:tblCellMar>'
           '</w:tblPr><w:tblGrid>' + ''.join(f'<w:gridCol w:w="{g}"/>' for g in grid) + '</w:tblGrid>']
    for n, row in enumerate(rows):
        out.append('<w:tr>' + ('<w:trPr><w:tblHeader/></w:trPr>' if n == 0 else ''))
        col = 0
        for cell, span in row:
            g = sum(grid[col:col + span])
            # Bold: the header row, and a cell across the whole table (a
            # group heading).
            bold = n == 0 or span == len(grid)
            rpr = '<w:rPr><w:b/></w:rPr>' if bold else ''
            text = f'<w:r>{rpr}<w:t xml:space="preserve">{escape(clean(cell))}</w:t></w:r>' if cell else ''
            merge = f'<w:gridSpan w:val="{span}"/>' if span > 1 else ''
            out.append(f'<w:tc><w:tcPr><w:tcW w:w="{g}" w:type="dxa"/>{merge}'
                       '<w:shd w:val="clear" w:color="auto" w:fill="CDD4E9"/></w:tcPr>'
                       f'<w:p><w:pPr><w:pStyle w:val="TableText"/></w:pPr>{text}</w:p></w:tc>')
            col += span
        out.append('</w:tr>')
    out.append('</w:tbl><w:p><w:pPr><w:pStyle w:val="TableAfter"/></w:pPr></w:p>')
    return ''.join(out)


def box_size(lines, indented):
    # Half-points. Consolas is about 0.55 em wide per character; shrink
    # wide blocks (ASCII tables) so their rows fit the box without wrapping.
    width = 440 if indented else 460
    widest = max((len(l) for l in lines), default=0)
    if widest * 0.55 * 10 <= width:
        return None
    return max(12, int(2 * width / (0.55 * widest)))


def parse(md):
    """Yield (kind, payload): title, heading, step (number, text), box (lines), body (text)."""
    lines = md.split('\n')
    i = 0
    while i < len(lines):
        l = lines[i]
        if l.startswith('```'):
            j = i + 1
            while not lines[j].startswith('```'):
                j += 1
            yield 'box', lines[i + 1:j]
            i = j + 1
            continue
        if not l.strip():
            i += 1
            continue
        if l.startswith('# '):
            yield 'title', l[2:].strip()
            i += 1
            continue
        if l.startswith('## '):
            yield 'heading', l[3:].strip()
            i += 1
            continue
        if l.startswith('### '):
            yield 'heading2', l[4:].strip()
            i += 1
            continue
        # A paragraph, step or bullet: lines up to a blank line, a fence,
        # or the next step or bullet.
        j = i + 1
        while j < len(lines) and lines[j].strip() and not lines[j].startswith(('```', '- ')) \
                and not re.match(r'^\d+\.\s', lines[j]):
            j += 1
        text = clean(' '.join(x.strip() for x in lines[i:j]))
        m = re.match(r'^(\d+)\.\s+(.*)$', text)
        if m:
            yield 'step', (int(m.group(1)), m.group(2))
        elif text.startswith('- '):
            yield 'bullet', text[2:]
        else:
            yield 'body', text
        i = j


def build(md):
    # numId 1 is the bullet list; each numbered list gets its own numId
    # after that, so numbering restarts at each list's first number.
    body, nums = [], []      # nums: start value of each numbered list
    in_list = False
    for kind, x in parse(md):
        if kind == 'title':
            body.append(para('Title', x))
        elif kind in ('heading', 'heading2'):
            body.append(para('Heading1' if kind == 'heading' else 'Heading2', x))
            in_list = False
        elif kind == 'step':
            if not in_list:
                nums.append(x[0])
                in_list = True
            body.append(para('Step', x[1], num=len(nums) + 1))
        elif kind == 'bullet':
            body.append(para('Bullet', x, num=1))
        elif kind == 'box' and table_rows(x):
            body.append(word_table(table_rows(x), in_list))
        elif kind == 'box':
            size = box_size(x, in_list)
            style = 'CommandIndented' if in_list else 'Command'
            body.extend(para(style, l, size=size) for l in x)
        else:
            body.append(para('BodyIndented' if in_list else 'BodyText', x))
    return ''.join(body), nums


LIGHT = '<w:rFonts w:ascii="Calibri Light" w:hAnsi="Calibri Light" w:cs="Calibri Light"/>'
MONO = '<w:rFonts w:ascii="Consolas" w:hAnsi="Consolas" w:cs="Consolas"/>'
# Matching shading and same-colored borders on adjacent paragraphs merge
# them into one box; the border space gives the box its padding.
BOX = ''.join(f'<w:{s} w:val="single" w:sz="4" w:space="4" w:color="{GRAY}"/>'
              for s in ('top', 'left', 'bottom', 'right'))
STYLES = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles {W}>
<w:docDefaults><w:rPrDefault><w:rPr>{LIGHT}<w:sz w:val="22"/><w:szCs w:val="22"/><w:lang w:val="en-US"/></w:rPr></w:rPrDefault>
<w:pPrDefault><w:pPr><w:spacing w:after="0" w:line="240" w:lineRule="auto"/></w:pPr></w:pPrDefault></w:docDefaults>
<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/><w:qFormat/></w:style>
<w:style w:type="paragraph" w:styleId="BodyText"><w:name w:val="Body Text"/><w:basedOn w:val="Normal"/><w:qFormat/>
  <w:pPr><w:spacing w:after="160"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="BodyIndented"><w:name w:val="Body Indented"/><w:basedOn w:val="BodyText"/><w:qFormat/>
  <w:pPr><w:spacing w:before="80" w:after="80"/><w:ind w:left="360"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/><w:basedOn w:val="Normal"/><w:next w:val="BodyText"/><w:qFormat/>
  <w:pPr><w:spacing w:after="360"/><w:jc w:val="center"/></w:pPr>
  <w:rPr>{LIGHT}<w:color w:val="000000"/><w:spacing w:val="-40"/><w:sz w:val="72"/><w:szCs w:val="72"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/><w:basedOn w:val="Normal"/><w:next w:val="BodyText"/><w:qFormat/>
  <w:pPr><w:keepNext/><w:spacing w:before="360" w:after="160"/><w:outlineLvl w:val="0"/></w:pPr>
  <w:rPr>{LIGHT}<w:color w:val="000000"/><w:spacing w:val="-40"/><w:sz w:val="48"/><w:szCs w:val="48"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading2"><w:name w:val="heading 2"/><w:basedOn w:val="Normal"/><w:next w:val="BodyText"/><w:qFormat/>
  <w:pPr><w:keepNext/><w:spacing w:before="240" w:after="120"/><w:outlineLvl w:val="1"/></w:pPr>
  <w:rPr>{LIGHT}<w:color w:val="000000"/><w:sz w:val="36"/><w:szCs w:val="36"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="TableText"><w:name w:val="Table Text"/><w:basedOn w:val="Normal"/><w:qFormat/>
  <w:pPr><w:spacing w:before="40" w:after="40"/></w:pPr><w:rPr><w:sz w:val="20"/><w:szCs w:val="20"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="TableAfter"><w:name w:val="Table After"/><w:basedOn w:val="Normal"/>
  <w:pPr><w:spacing w:after="120"/></w:pPr><w:rPr><w:sz w:val="12"/><w:szCs w:val="12"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Bullet"><w:name w:val="Bullet"/><w:basedOn w:val="Normal"/><w:qFormat/>
  <w:pPr><w:spacing w:after="80"/><w:ind w:left="360" w:hanging="360"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Step"><w:name w:val="Step"/><w:basedOn w:val="Normal"/><w:qFormat/>
  <w:pPr><w:keepNext/><w:spacing w:before="160" w:after="80"/><w:ind w:left="360" w:hanging="360"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Command"><w:name w:val="Command"/><w:basedOn w:val="Normal"/><w:qFormat/>
  <w:pPr><w:keepNext/><w:keepLines/><w:pBdr>{BOX}</w:pBdr><w:shd w:val="clear" w:color="auto" w:fill="{GRAY}"/>
  <w:spacing w:before="0" w:after="0"/><w:ind w:left="80" w:right="80"/></w:pPr>
  <w:rPr>{MONO}<w:sz w:val="20"/><w:szCs w:val="20"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="CommandIndented"><w:name w:val="Command Indented"/><w:basedOn w:val="Command"/><w:qFormat/>
  <w:pPr><w:ind w:left="440" w:right="80"/></w:pPr></w:style>
</w:styles>'''


def numbering(starts):
    parts = ['<w:abstractNum w:abstractNumId="1"><w:multiLevelType w:val="singleLevel"/>'
             '<w:lvl w:ilvl="0"><w:start w:val="1"/><w:numFmt w:val="bullet"/>'
             '<w:lvlText w:val="•"/><w:lvlJc w:val="left"/>'
             '<w:pPr><w:ind w:left="360" w:hanging="360"/></w:pPr></w:lvl></w:abstractNum>']
    for n, start in enumerate(starts, 2):
        parts.append(f'<w:abstractNum w:abstractNumId="{n}"><w:multiLevelType w:val="singleLevel"/>'
                     f'<w:lvl w:ilvl="0"><w:start w:val="{start}"/><w:numFmt w:val="decimal"/>'
                     '<w:lvlText w:val="%1."/><w:lvlJc w:val="left"/>'
                     '<w:pPr><w:ind w:left="360" w:hanging="360"/></w:pPr></w:lvl></w:abstractNum>')
    parts += [f'<w:num w:numId="{n}"><w:abstractNumId w:val="{n}"/></w:num>'
              for n in range(1, len(starts) + 2)]
    return f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:numbering {W}>{"".join(parts)}</w:numbering>'


CONTENT_TYPES = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
<Override PartName="/word/numbering.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml"/>
</Types>'''
ROOT_RELS = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>'''
DOC_RELS = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/numbering" Target="numbering.xml"/>
</Relationships>'''
SECT = ('<w:sectPr><w:pgSz w:w="12240" w:h="15840"/>'
        '<w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" '
        'w:header="720" w:footer="720" w:gutter="0"/></w:sectPr>')


def main(src, out):
    body, starts = build(open(src, encoding='utf-8').read())
    document = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                f'<w:document {W}><w:body>{body}{SECT}</w:body></w:document>')
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml', CONTENT_TYPES)
        z.writestr('_rels/.rels', ROOT_RELS)
        z.writestr('word/document.xml', document)
        z.writestr('word/styles.xml', STYLES)
        z.writestr('word/numbering.xml', numbering(starts))
        z.writestr('word/_rels/document.xml.rels', DOC_RELS)
    print('wrote', out)


def all_docs(out_dir):
    # doc/ is the folder above this script; skip doc/tools and doc/examples.
    doc = Path(__file__).resolve().parent.parent
    failed = 0
    for src in sorted(doc.rglob('*.md')):
        rel = src.relative_to(doc)
        if rel.parts[0] in ('tools', 'examples'):
            continue
        out = Path(out_dir) / rel.with_suffix('.docx')
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            main(src, out)
        except PermissionError:
            print('skipped', out, '(open in Word? close it and run again)')
            failed += 1
    return failed


if __name__ == '__main__':
    if sys.argv[1:2] == ['--all']:
        sys.exit(1 if all_docs(sys.argv[2]) else 0)
    main(sys.argv[1], sys.argv[2])
