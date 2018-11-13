import os
import glob
import json
def get_las_json(db="raw_reads",las_json="las_path.json"):
    curent_dir = os.getcwd()

    las  = glob.glob('{}/{}.*.{}.*.las'.format(curent_dir, db, db))

    json_las = json.dumps(las,indent=2)
    with open(las_json,"w") as f:
        f.writelines(json_las)

if __name__ == "__main__":
    get_las_json()