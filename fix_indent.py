with open('/workspace/app/llm/client.py', 'r') as f:
    lines = f.readlines()

new_lines = []
in_block = False
for i, line in enumerate(lines):
    if "with _HTTP_CLIENT.stream" in line:
        in_block = True
        new_lines.append(line)
        continue
    if in_block:
        if line.strip() == "except LLMError:":
            in_block = False
            new_lines.append(line)
            continue
        if line.startswith("                ") or line.strip() == "":
            if line.strip() == "":
                new_lines.append(line)
            else:
                new_lines.append(line[8:])
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open('/workspace/app/llm/client.py', 'w') as f:
    f.writelines(new_lines)
