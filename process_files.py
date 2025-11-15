import re
import os
from pathlib import Path
import ast
import astor

def remove_comments_and_docstrings(content):
    try:
        tree = ast.parse(content)
        
        def remove_docstrings(node):
            if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)):
                if ast.get_docstring(node):
                    node.body = [child for child in node.body if not (isinstance(child, ast.Expr) and isinstance(child.value, ast.Str))]
            for child in ast.iter_child_nodes(node):
                remove_docstrings(child)
        
        remove_docstrings(tree)
        
        result = astor.to_source(tree)
        
        lines = result.split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.rstrip()
            if '#' in stripped:
                in_string = False
                quote_char = None
                i = 0
                while i < len(stripped):
                    char = stripped[i]
                    if not in_string:
                        if char in ['"', "'"]:
                            if i + 2 < len(stripped) and stripped[i:i+3] == char * 3:
                                in_string = True
                                quote_char = char * 3
                                i += 3
                                continue
                            elif i + 1 < len(stripped) and stripped[i+1] in ['"', "'"]:
                                i += 2
                                continue
                            else:
                                in_string = True
                                quote_char = char
                                i += 1
                                continue
                        elif char == '#':
                            stripped = stripped[:i].rstrip()
                            break
                    else:
                        if quote_char == char * 3:
                            if i + 2 < len(stripped) and stripped[i:i+3] == quote_char:
                                in_string = False
                                quote_char = None
                                i += 3
                                continue
                        elif quote_char == char:
                            if i > 0 and stripped[i-1] != '\\':
                                in_string = False
                                quote_char = None
                    i += 1
            cleaned_lines.append(stripped)
        
        return '\n'.join(cleaned_lines)
    except:
        lines = content.split('\n')
        result = []
        in_triple_quote = False
        triple_quote_char = None
        
        for line in lines:
            stripped = line
            if not in_triple_quote:
                if '"""' in line:
                    parts = line.split('"""')
                    if len(parts) > 1:
                        in_triple_quote = True
                        triple_quote_char = '"'
                        if parts[0].strip() and not parts[0].strip().endswith('='):
                            stripped = parts[0].rstrip()
                        else:
                            stripped = ''
                elif "'''" in line:
                    parts = line.split("'''")
                    if len(parts) > 1:
                        in_triple_quote = True
                        triple_quote_char = "'"
                        if parts[0].strip() and not parts[0].strip().endswith('='):
                            stripped = parts[0].rstrip()
                        else:
                            stripped = ''
                else:
                    if '#' in stripped:
                        in_string = False
                        quote_char = None
                        i = 0
                        while i < len(stripped):
                            char = stripped[i]
                            if not in_string:
                                if char in ['"', "'"]:
                                    if i + 2 < len(stripped) and stripped[i:i+3] == char * 3:
                                        in_string = True
                                        quote_char = char * 3
                                        i += 3
                                        continue
                                    else:
                                        in_string = True
                                        quote_char = char
                                        i += 1
                                        continue
                                elif char == '#':
                                    stripped = stripped[:i].rstrip()
                                    break
                            else:
                                if quote_char == char * 3:
                                    if i + 2 < len(stripped) and stripped[i:i+3] == quote_char:
                                        in_string = False
                                        quote_char = None
                                        i += 3
                                        continue
                                elif quote_char == char:
                                    if i > 0 and stripped[i-1] != '\\':
                                        in_string = False
                                        quote_char = None
                            i += 1
            else:
                if triple_quote_char * 3 in line:
                    in_triple_quote = False
                    triple_quote_char = None
                    stripped = ''
                else:
                    stripped = ''
            
            if stripped or (result and result[-1].strip()):
                result.append(stripped)
        
        return '\n'.join(result)

def process_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        content = content.replace('EvTexture', 'EVTSR')
        content = content.replace('evtexture', 'EVTSR')
        content = content.replace('EVTEXTURE', 'EVTSR')
        
        content = remove_comments_and_docstrings(content)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    root = Path('.')
    py_files = list(root.rglob('*.py'))
    
    print(f"Found {len(py_files)} Python files")
    
    for py_file in py_files:
        if py_file.name == 'process_files.py':
            continue
        print(f"Processing {py_file}...")
        process_file(py_file)
    
    print("Done!")

if __name__ == '__main__':
    main()

