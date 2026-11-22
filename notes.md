# Notes

## Cleaning the `author` field (`clean_author` in `extractor_wiki.py`)

### The problem

The raw `| author = ...` value from the wiki dump is full of markup, and it comes in several shapes:

```
[[A. J. Cronin]]
[[J. R. R. Tolkien (writer)|Tolkien]]
''[[Terry Pratchett]]'' and [[Neil Gaiman]]
{{ubl|[[Ann Smith (writer)|Ann Smith]]|John Doe}}
{{plainlist|
* [[Ann Smith]]
* John Doe
}}
Dick King-Smith&lt;ref&gt;{{cite news|title=x}}&lt;/ref&gt;
C.&amp;nbsp;S. Lewis &amp; J. R. R. Tolkien
```

(The dump stores `<` and `>` as `&lt;` and `&gt;`, which is why the tags look like that.)

The first version had **one regex per case**: 17 `regexp_replace` calls, 2 `rlike` checks and 2
`when` branches (one for `{{ubl}}` lists, one for `{{plainlist}}` lists). Every new case meant one more step,
and the order of the steps mattered.

### The idea: every piece of text is one of only two things

Looking at all those steps, each one did one of only two things:

| Kind | Examples | What we do with it |
|---|---|---|
| **Noise**: markup that is never part of a name | `<!-- comments -->`, `<ref>…</ref>`, `<small>`, `&quot;`, `&nbsp;`, `''` italics, `[[` `]]`, the `\|Label]]` of a link, `(writer)`, template names like `{{ubl\|` and `{{plainlist`, `}}`, `*` bullets, extra spaces/tabs | replace with **one space** |
| **Separator**: what stands between two authors | `,` &nbsp; `\|` &nbsp; new line &nbsp; `<br>` &nbsp; `&amp;` &nbsp; `and` | **split** the value there |

Whatever is left after that is names. So instead of 17 steps, there are **2 regexes**:

```python
col = F.regexp_replace(col, AUTHOR_NOISE_RE, ' ')                         # 1. noise -> one space
authors = F.transform(F.split(col, AUTHOR_SEP_RE), lambda a: F.trim(a))   # 2. split at separators
authors = F.array_remove(authors, '')                                     # drop empty pieces
```

### Regex 1: `AUTHOR_NOISE_RE` (noise to one space)

```
(?is)(?:
    &lt;!--.*?(?:--&gt;|$)                              <!-- comment -->, also a cut-off one
  | &lt;ref[^&]*?/&gt; | &lt;ref.*?&lt;/ref&gt; | &lt;ref.*    <ref .../>, <ref>...</ref>, cut-off <ref...
  | &lt;/?small\s*&gt; | &(?:amp;)?(?:quot|nbsp);        <small> </small>, &quot; &nbsp;
  | \{\{[^|{}\n]*\|? | \}\}                             a template's name ({{ubl|, {{plainlist) and }}
  | \|[^\[\]]*\]\] | [\[\]]                             |Label]] of [[Target|Label]], then any [ or ]
  | '{2,} | "{2,} | \* | \([^)]*\)                      '' ''' italics/bold, * bullet, (writer)
  | [^\S\n]                                            a space or tab, but NOT a new line
)+
```

How to read it:

- It is one big **alternation** (`a | b | c | ...`): "match any of these kinds of noise".
- The `(?:...)+` around it means **a run of several noise pieces in a row is one match**, so the run
  becomes a single space. For example ` (writer)|Ann Smith]]` is a space, `(writer)` and `|Ann Smith]]`
  in a row, and the whole run becomes one `' '`.
- `(?i)` makes it case-insensitive (`<REF>`, `{{Plainlist`), and `(?s)` lets `.` match new lines too, because a
  comment or `<ref>` can span several lines.
- **Spaces and tabs are noise, new lines are not** (`[^\S\n]` = "whitespace except `\n`"). A new line has to
  survive step 1 because in a `{{plainlist}}` it separates two authors.

Why some things are written the way they are:

- **`&(?:amp;)?(?:quot|nbsp);` only removes `&quot;` and `&nbsp;`, never a bare `&amp;`.** A bare `&amp;` is a
  separator (`A &amp; B`), so it has to stay for step 2. The double-escaped `&amp;nbsp;` is removed here
  as well, so `C.&amp;nbsp;S. Lewis` does not turn into a separator and get split into `C.` and `nbsp;S. Lewis`.
- **`\{\{[^|{}\n]*\|?` removes the name of *any* template**: `{{ubl|`, `{{unbulleted list|`, `{{plainlist|`,
  `{{Plainlist` + new line, `{{nowrap|`. Only the arguments stay. That is what made the special ubl/plainlist
  branches unnecessary.
- **`\|[^\[\]]*\]\]` removes the `|Label]]` part of a link** (from the `|` to the `]]`, without crossing another `[`
  or `]`). So `[[Target|Label]]` keeps the **Target**, and every `|` that is still there afterwards is a list
  separator, never part of a link.
- **`(writer)`, `(novelist)`, `(ed.)` go away**: `\([^)]*\)` removes them with their brackets.

### Regex 2: `AUTHOR_SEP_RE` (split at separators)

```
(?i)\s*(?: &lt;br\s*/?&gt; | &amp; | \band\b | [,|\n] )\s*
```

- `<br>`, `<br/>`, `<br />`, then `&amp;`, then the word `and`, then any of `,` `|` new line.
- `\s*` on both sides **eats the spaces and new lines around the separator**, so the pieces come out
  almost clean. (`F.trim` only removes spaces, not `\n`, so a new line left at the edge of a name would
  stay in the result. Here that cannot happen.)
- `\band\b` = `and` as a **whole word** (`\b` = word boundary). So `Ann and Bob` is split, but
  `Alexander` and `Andrew` are not, because `and` is in the middle of the word there.
- `F.split` accepts a regex, so splitting is the second regex. No separate "replace separators by `,`, then
  split on `,`" step is needed.

At the end, `trim` cuts the spaces off each name and `array_remove(..., '')` drops the empty pieces (from a
separator at the start or end, or from two separators in a row).

### Examples, step by step

`step 1` is the value after the noise regex (spaces shown as they are, `\n` = new line).

| Raw value | After step 1 | Result |
|---|---|---|
| `[[A. J. Cronin]]` | `␣A. J. Cronin␣` | `['A. J. Cronin']` |
| `[[J. R. R. Tolkien (writer)\|Tolkien]]` | `␣J. R. R. Tolkien␣` | `['J. R. R. Tolkien']` |
| `''[[Terry Pratchett]]'' and [[Neil Gaiman]]` | `␣Terry Pratchett and Neil Gaiman␣` | `['Terry Pratchett', 'Neil Gaiman']` |
| `{{ubl\|[[Ann Smith (writer)\|Ann Smith]]\|John Doe}}` | `␣Ann Smith \|John Doe␣` | `['Ann Smith', 'John Doe']` |
| `{{plainlist\|\n* [[Ann Smith]]\n* John Doe\n}}` | `␣\n Ann Smith \n John Doe\n␣` | `['Ann Smith', 'John Doe']` |
| `* Ann Smith\n* John Doe` | `␣Ann Smith\n John Doe` | `['Ann Smith', 'John Doe']` |
| `Dick King-Smith&lt;ref&gt;{{cite news\|title=x}}&lt;/ref&gt;` | `Dick King-Smith␣` | `['Dick King-Smith']` |
| `Vladimir Nabokov\n&lt;!-- do not change` | `Vladimir Nabokov\n␣` | `['Vladimir Nabokov']` |
| `C.&amp;nbsp;S. Lewis &amp; J. R. R. Tolkien` | `C. S. Lewis &amp; J. R. R. Tolkien` | `['C. S. Lewis', 'J. R. R. Tolkien']` |
| `Ann Smith&lt;br /&gt;John Doe` | *(no noise, unchanged)* | `['Ann Smith', 'John Doe']` |
| `Ann Smith &lt;small&gt;(ed.)&lt;/small&gt;` | `Ann Smith␣` | `['Ann Smith']` |
| `Alexander Dumas` | *(unchanged)* | `['Alexander Dumas']` (not split at "and") |
| `Andrew Lang, Ann Smith` | *(unchanged)* | `['Andrew Lang', 'Ann Smith']` |
| `{{nowrap\|Ann Smith}}` | `␣Ann Smith␣` | `['Ann Smith']` |

A closer look at the `{{ubl}}` example:

```
{{ubl|[[Ann Smith (writer)|Ann Smith]]|John Doe}}
^^^^^^^^                                            '{{ubl|' + '[['                  -> ' '
        Ann Smith                                   name, kept
                 ^^^^^^^^^^^^^^^^^^^^^^             ' ' + '(writer)' + '|Ann Smith]]' -> ' '  (one run)
                                       |            NOT noise: not followed by ]] -> stays
                                        John Doe    name, kept
                                                ^^  '}}'                              -> ' '

after step 1:  " Ann Smith |John Doe "
split at '|':  [" Ann Smith", "John Doe "]   (the \s* around the separator eats the space before |)
trim:          ["Ann Smith", "John Doe"]
```

### Differences from the old version

1. **A new line always separates two authors**, not only inside `{{plainlist}}`. Infobox values put one
   author per line, so this is what we want. A single name broken over two lines would become two entries,
   but that should be rare.
2. **`|` separates authors everywhere**, not only inside `{{ubl}}`. `{{nowrap|Ann Smith}}` now gives `Ann Smith`
   (the old version kept the braces). `{{lang|fr|Name}}` would give a junk `fr` next to `Name`.
3. **Removed markup becomes a space, not nothing**, so `C.&nbsp;S. Lewis` becomes `C. S. Lewis` (before it was
   `C.S. Lewis`). `&nbsp;` really is a space, so this is the more correct form.

### The same idea elsewhere

`value_list` (genre) already works halfway like this (`VALUE_NOISE_RE` + `LIST_SEP_RE`) and could be rewritten
the same way: one noise regex, then a split.


## Example search queries (`searcher.py`)

The searcher uses Lucene's own query syntax (`MultiFieldQueryParser`): `AND`, `OR`, `NOT`, `"phrase"`, `( )` and
`field:word`. The operators must be in capitals. Run this once in PowerShell in `cast_2`, so each query fits on one
line:

```powershell
function search { docker run --rm -it -v "${PWD}:/work" -w /work coady/pylucene python searcher.py @args }
```

### Operators

```powershell
search war AND peace                          # both words
search dragon OR wizard                       # at least one of them
search dragon NOT tolkien                     # dragon, without tolkien
search '\"lord of the rings\"'                # a phrase: these words together, in this order
```

### A few words

Plain words are joined with OR. A book with more of them scores higher, and a title with the words in this
order gets a bonus.

```powershell
search lighthouse keeper storm
```

### Range of years

```powershell
search detective --from 1920 --to 1940
```

### Category

There is no `--category` option, so use Lucene's `field:` syntax:

```powershell
search 'categories:\"science fiction\"'
```

### Author

```powershell
search authors:tolkien
search 'authors:\"agatha christie\"'
```

### Combined

```powershell
search '(authors:christie OR authors:doyle) AND murder NOT poison' --from 1900 --to 1950 --language english
```

### Notes

- The `\"` inside `'...'` is only needed in Windows PowerShell 5.1. Without it, PowerShell drops the inner
  quotes and the phrase becomes plain words.
- The searcher prints the parsed query on the `query:` line. A phrase shows as `title:"lord of the rings"`
  and a NOT shows as `-(...)`.
- `NOT war` alone finds nothing: Lucene's NOT only removes books, so it needs a positive part (`dragon NOT war`).
- Mixing `AND` and `OR` without brackets doesn't follow the usual precedence, so use brackets.




