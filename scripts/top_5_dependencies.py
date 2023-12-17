import argparse
from script_interface import ScriptInterface
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType
from itertools import combinations


class Top5Dependencies(ScriptInterface):
    def __init__(self):
        super().__init__('Obtain the top 5 dependencies used by language', language=True)
        self.language_filter = self.parser.parse_args().language
        self.log.info('Filtering by language: %s', self.language_filter)
        self.language_file_endings = {
                'C': '.c',
                'D': '.d',
                'Perl': '.pl',
                'Python': '.py',
                'Shell': '.sh',
                'R': '.r',
                'C#': '.cs',
                'M': '.m',
                'Smalltalk': '.st',
                'C++': '.cpp',
                'Objective-C': '.m',
                'PHP': '.php',
                'CSS': '.css',
                'HTML': '.html',
                'Java': '.java',
                'JavaScript': '.js',
                'Lua': '.lua',
                'Pascal': '.pas',
                'Ruby': '.rb',
                'VHDL': '.vhd',
                'Erlang': '.erl',
                'F#': '.fs',
                'Haskell': '.hs',
                'Idris': '.idr',
                'Makefile': '.mk',
                'PowerShell': '.ps1',
                'Prolog': '.pl',
                'Protocol Buffer': '.proto',
                'PureScript': '.purs',
                'Scala': '.scala',
                'TeX': '.tex',
                'FLUX': '.fx',
                'GLSL': '.glsl',
                'Objective-C++': '.mm',
                'CMake': '.cmake',
                'OCaml': '.ml',
                'Tcl': '.tcl',
                'PureBasic': '.pb',
                'Emacs Lisp': '.el',
                'VimL': '.vim',
                'CoffeeScript': '.coffee',
                'Go': '.go',
                'Nginx': '.conf',
                'SaltStack': '.sls',
                'Inno Setup': '.iss',
                'M4': '.m4',
                'Ragel': '.rl',
                'Roff': '.roff',
                'Rich Text Format': '.rtf',
                'Scheme': '.scm',
                'Coq': '.v',
                'Standard ML': '.sml',
                'Verilog': '.v',
                'IDL': '.idl',
                'TypeScript': '.ts',
                'RenderScript': '.rs',
                'QMake': '.pro',
                'NSIS': '.nsi',
                'Groff': '.man',
                'Groovy': '.groovy',
                'XSLT': '.xslt',
                'Cucumber': '.feature',
                'Liquid': '.liquid',
                'Logos': '.xi',
                'Ragel in Ruby Host': '.rl',
                'Yacc': '.y',
                'Lex': '.l',
                'SQF': '.sqf',
                'Cuda': '.cu',
                'Dockerfile': 'Dockerfile',
                'Hack': '.hack',
                'Matlab': '.mat',
                'Thrift': '.thrift',
                'PLpgSQL': '.sql',
                'Smarty': '.tpl',
                'QML': '.qml',
                'Mathematica': '.nb',
                'FORTRAN': '.f',
                'Harbour': '.hb',
                'Less': '.less',
                'Starlark': '.star',
                'Gnuplot': '.gnuplot',
                'Fortran': '.f',
                'Perl6': '.p6',
                'Scilab': '.sci',
                'PostScript': '.ps',
                'PLSQL': '.sql',
                'Cython': '.pyx',
                'Delphi': '.pas',
                'OpenEdge ABL': '.p',
                'Vim script': '.vim',
                'SQL': '.sql',
                'Processing': '.pde',
                'Common Lisp': '.lisp',
                'Swift': '.swift',
                'Jupyter Notebook': '.ipynb',
                'Clojure': '.clj',
                'Haxe': '.hx',
                'Perl 6': '.p6',
                'XS': '.xs',
                'COBOL': '.cob',
                'Forth': '.4th',
                'Mercury': '.m',
                'CartoCSS': '.mss',
                'RobotFramework': '.robot',
                'sed': '.sed',
                'Cool': '.cl',
                'Visual Basic': '.vb',
                'HLSL': '.hlsl',
                'ShaderLab': '.shader',
                'Rust': '.rs',
                'Vue': '.vue',
                'Nix': '.nix',
                'SAS': '.sas',
                'PigLatin': '.pig',
                'ASP': '.asp',
                'SQLPL': '.sql',
                'Puppet': '.pp',
                'ColdFusion': '.cfm',
                'Lasso': '.lasso',
                'Assembly': '.asm',
                'Eiffel': '.e',
                'SuperCollider': '.sc',
                'Awk': '.awk',
                'DOT': '.dot',
                'XML': '.xml',
                'Nu': '.nu',
                'Racket': '.rkt',
                'SourcePawn': '.sp',
                'UnrealScript': '.uc',
                'Bison': '.y',
                'Batchfile': '.bat',
                'ApacheConf': '.conf',
                'ActionScript': '.as',
                'GCC Machine Description': '.md',
                'Ada': '.ada',
                'Arduino': '.ino',
                'Crystal': '.cr',
                'Elixir': '.ex',
                'Kotlin': '.kt',
                'CLIPS': '.clp',
                'DIGITAL Command Language': '.com',
                'Module Management System': '.mms',
                'AppleScript': '.applescript',
                'Boo': '.boo',
                'Io': '.io',
                'NewLisp': '.nl',
                'xBase': '.prg',
                'Arc': '.arc',
                'GAP': '.g',
                'Chapel': '.chpl',
                'Julia': '.jl',
                'Nimrod': '.nim',
                'Meson': '.meson',
                'Bro': '.bro',
                'Stan': '.stan',
                'Dart': '.dart',
                'Gettext Catalog': '.po',
                'Cap\'n Proto': '.capnp',
                'DM': '.dm',
                'EJS': '.ejs',
                'Elm': '.elm',
                'HCL': '.hcl',
                'Web Ontology Language': '.owl',
                'SCSS': '.scss',
                'Handlebars': '.hbs',
                'TSQL': '.tsql',
                'Mako': '.mako',
                'Modelica': '.mo',
                'LiveScript': '.ls',
                'Gherkin': '.feature',
                'VCL': '.vcl',
                'Isabelle': '.thy',
                'SMT': '.smt2',
                'Stylus': '.styl',
                'NASL': '.nasl',
                'jq': '.jq',
                'Jinja': '.j2',
                'Vim Script': '.vim',
                'YASnippet': '.yasnippet',
                'FreeMarker': '.ftl',
                'Rebol': '.reb',
                'Gosu': '.gs',
                'Haml': '.haml',
                'GDB': '.gdb',
                'Terra': '.tf',
                'MoonScript': '.moon',
                'Nim': '.nim',
                'Vim Snippet': '.snippets',
                'AMPL': '.mod',
                'MATLAB': '.mat',
                'Metal': '.metal',
                'Pawn': '.pwn',
                'SWIG': '.swig',
                'API Blueprint': '.apib',
                'Agda': '.agda',
                'Apex': '.cls',
                'KiCad': '.kicad_pcb',
                'Inform 7': '.ni',
                'PAWN': '.pwn',
                'Raku': '.raku',
                'Xojo': '.xojo_code',
                'ANTLR': '.g4',
                'Blade': '.blade',
                'Vala': '.vala',
                'Eagle': '.sch',
                'Augeas': '.aug',
                'Xtend': '.xtend',
                'Zimpl': '.zimpl',
                'kvlang': '.kv',
                'ASP.NET': '.aspx',
                'Game Maker Language': '.gml',
                'AspectJ': '.aj',
                'TLA': '.tla',
                'BitBake': '.bb',
                'LabVIEW': '.vi',
                'Pan': '.pan',
                'Stata': '.do',
                'SystemVerilog': '.sv',
                'Slash': '.sl',
                'q': '.q',
                'BlitzBasic': '.bb',
                'GSC': '.gsc',
                'OpenQASM': '.qasm',
                'eC': '.ec',
                'GDScript': '.gd',
                'Squirrel': '.nut',
                'Ballerina': '.bal',
                'E': '.e',
                'Max': '.max',
                'EmberScript': '.em',
                'DTrace': '.d',
                'LLVM': '.ll',
                'KiCad Layout': '.kicad_pcb',
                'Mustache': '.mustache',
                'Clean': '.icl',
                'Tea': '.tea',
                'Pure Data': '.pd',
                'AGS Script': '.asc',
                'Diff': '.diff',
                'MTML': '.mtml',
                'wisp': '.wisp',
                'Procfile': '.procfile',
                'Pug': '.pug',
                'Cycript': '.cy',
                'Fancy': '.fy',
                'Hy': '.hy',
                'Sass': '.sass',
                '1C Enterprise': '.1ce',
                'Visual Basic .NET': '.vb',
                'ooc': '.ooc',
                'Euphoria': '.ex',
                'RPC': '.proto',
                'nesC': '.nc',
                'Slice': '.ice',
                'OpenSCAD': '.scad',
                'VBA': '.vba',
                'XQuery': '.xq',
                'Opa': '.opa',
                'LOLCODE': '.lol',
                'Mask': '.mask',
                'Pony': '.pony',
                'Sage': '.sage',
                'Futhark': '.fut',
                'Frege': '.fr',
                'Propeller Spin': '.spin',
                'POV-Ray SDL': '.pov',
                'XC': '.xc',
                'JSONiq': '.jq',
                'RAML': '.raml',
                'UrWeb': '.ur',
                'Modula-2': '.mod',
                'Monkey C': '.mc',
                'WebIDL': '.webidl',
                'ZenScript': '.zs',
                'Limbo': '.b',
                'mupad': '.mu',
                'Kit': '.kit',
                'P4': '.p4',
                'Turing': '.t',
                'Parrot': '.parrot',
                'Fantom': '.fan',
                'Fennel': '.fnl',
                'Modula-3': '.m3',
                'Genshi': '.kid',
                'Slim': '.slim',
                'hoon': '.hoon',
                'LilyPond': '.ly',
                'NCL': '.ncl',
                'SmPL': '.cocci',
                'NetLogo': '.nlogo',
                'MQL4': '.mq4',
                'YARA': '.yara',
                'MQL5': '.mq5',
                'XProc': '.xpl',
                'WebAssembly': '.wat',
                'G-code': '.gcode',
                'MAXScript': '.ms',
                'MLIR': '.mlir',
                'Csound': '.csd',
                'HiveQL': '.q',
                'Clarion': '.clw',
                'Zig': '.zig',
                'ZAP': '.zap',
                'Nextflow': '.nf',
                'ChucK': '.ck',
                'Csound Score': '.sco',
                'Nemerle': '.n',
                'Jasmin': '.j',
                'Red': '.red',
                'Singularity': '.def',
                'IGOR Pro': '.ipf',
                'Objective-J': '.j',
                'PicoLisp': '.l',
                'Zephir': '.zep',
                'Component Pascal': '.cp',
                'CWeb': '.w',
                'Rascal': '.rsc',
                'mIRC Script': '.mrc',
                'Graphviz (DOT)': '.dot',
                'Oz': '.oz',
                'DCPU-16 ASM': '.dasm',
                'LSL': '.lsl',
                'Solidity': '.sol',
                'Click': '.click',
                'Dylan': '.dylan',
                'HyPhy': '.bf',
                'VBScript': '.vbs',
                'J': '.ijs',
                'Polar': '.polar',
                'Oxygene': '.oxygene',
                'Jsonnet': '.jsonnet',
                'Promela': '.pml',
                'LFE': '.lfe',
                'Self': '.self',
                'Csound Document': '.orc',
                'AL': '.al',
                'Brightscript': '.brs',
                'Classic ASP': '.asp',
                'Svelte': '.svelte',
                'Twig': '.twig',
                'JFlex': '.jflex',
                'Ren\'Py': '.rpy',
                'Myghty': '.myt',
                'SRecode Template': '.srt',
                'PEG.js': '.pegjs',
                'F*': '.fst',
                'Ox': '.ox',
                'APL': '.apl',
                'Alloy': '.als',
                'AutoHotkey': '.ahk',
                'AutoIt': '.au3',
                'BlitzMax': '.bmx',
                'Ceylon': '.ceylon',
                'Cirru': '.cirru',
                'Factor': '.factor',
                'Golo': '.golo'
        }

    def get_language_repos(self):
        """
        Get all repo_names for the target language from the 'languages' table.
        :returns: Spark DataFrame with the repo_names for language_filter.
        """
        languages_df = self.get_table('languages')

        languages_df = languages_df.withColumn('language_name', f.from_json('language_name', ArrayType(StringType())))

        # Filter repositories for the target language. Alias to sample_repo_name to allow later join operation
        language_repos_df = languages_df.filter(f.array_contains('language_name', self.language_filter)).select(
                f.col('repo_name').alias('sample_repo_name')
        )

        return language_repos_df

    def get_top_dependencies(self):
        """
        Analyze files in the 'contents' table to count dependencies for language_filter.
        :returns: Spark DataFrame with the top 5 dependencies of that language.
        """
        contents_df = self.get_table('contents')

        # Filter contents for the repositories of the target language
        language_repos_df = self.get_language_repos()
        language_contents_df = contents_df.join(language_repos_df, 'sample_repo_name')

        # Filter files where regex matches import and language file ending
        regex = r'(import|include)\s+(\w+)'
        file_ending = self.language_file_endings.get(self.language_filter, '')
        import_files_df = language_contents_df.filter(
            (f.col('content').rlike(regex)) & (f.col('sample_path').endswith(file_ending))
        )

        # Extract dependencies from the import statements
        dependencies_df = import_files_df.select(f.expr(f"regexp_extract(content, '{regex}', 1)").alias('dependency'))

        # Count occurrences of each dependency
        dependency_counts_df = dependencies_df.groupBy('dependency').agg(f.count('*').alias('count'))

        # Get the top 5 dependencies
        top_dependencies_df = dependency_counts_df.orderBy(f.desc('count')).limit(5)

        return top_dependencies_df

    def process_data(self):
        # Obtain multi-language statistics
        top5_dependencies_df = self.get_top_dependencies()

        # Log and print the combined result (if in test mode)
        if self.test_mode:
            top5_dependencies_df.show(truncate=False)
            self.log.info('Stats: \n%s', top5_dependencies_df.toPandas())

        # Save the combined result to a CSV file
        self.save_data(top5_dependencies_df, 'top_5_dependencies_' + self.language_filter)


if __name__ == "__main__":
    multi_language_repos = Top5Dependencies()
    multi_language_repos.run()