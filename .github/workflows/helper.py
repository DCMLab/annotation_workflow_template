import json
import argparse
import re
import os
def create_new_tag(tag):
    if not (re.match(r'^v\d', tag)):
        raise Exception(f'tag: {tag} is not giving in the correct format e.i v0.0')
    
    # Notice that this will make tag version of three digits become two digits
    # e.i 3.2.1 -> 3.3
    digits_tags = (re.match(r'^v\d+.\d+', tag)).group()[1::].split('.')
    if len(digits_tags) != 2:
        raise Exception(f'tag: {tag} must contain two version digits')
    
    new_digit = int(digits_tags[1]) + 1
    return "v" + str(digits_tags[0]) + "." + str(new_digit)

def store_tag(tag):
    with open(os.environ['GITHUB_OUTPUT'], 'a') as fh:
        print(f'new_tag={tag}', file=fh)

def update_files_with_tag(old_tag, new_tag):
    # This function needs to take care of updating 
    # .zenodo.json and CITATION.cff
    # TO-DO: make zenodo.json robust to search for version tags
    if os.path.isfile(".zenodo.json"):
        try:
            with open(".zenodo.json", "r") as f:
                data = json.load(f)

            data["version"] = data["version"].replace(old_tag,new_tag)
            data["title"] = data["title"].replace(old_tag,new_tag)

            with open(".zenodo.json", "w") as f:
                json.dump(data, f)
            
        except Exception as e:
            print(e)

    if os.path.isfile("CITATION.cff"):
        try:
            with open("CITATION.cff", "r", encoding="utf-8") as file:
                citation = file.read()
            modified_citation = citation.replace(old_tag, new_tag)
            with open("CITATION.cff", "w", encoding="utf-8") as file:
                file.write(modified_citation)                
        except Exception as e:
            print(e)

def main(args):
    tag = args.tag
    new_tag = "v2.0"
    if not tag:
        print(f"Warning: a latest release with a tag does not exist in current repository, starting from {new_tag}")
    else:
        new_tag = create_new_tag(tag)
        print(f"Repository with tag: {tag}, creating a new tag with: {new_tag}")
        update_files_with_tag(tag,new_tag)
    store_tag(new_tag)

def run():
    args = parser.parse_args()
    main(args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tag", type=str,
        help="Require: latest tag",
        required=True
    )
    run()