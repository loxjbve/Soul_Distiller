import glob
import re

for file in glob.glob("app/templates/*.html"):
    with open(file, "r") as f:
        content = f.read()
    matches = re.findall(r'[\u4e00-\u9fa5]+', content)
    if matches:
        print(f"{file}: {set(matches)}")
