import json
def generate_las_json():
    with open("las_path","r") as f:
        data = f.readlines()[0]
        new_data = data.replace(" ", "").replace("[","").replace("]","")
        gather_las = new_data.strip().split(",")
    total_las_json = list()
    for las in gather_las:
        with open(las,'r') as f:
            single_las = json.loads(f.read())
            for las in single_las:
                total_las_json.append(las)
    total_json = json.dumps(total_las_json,indent=2)
    with open("gather_las.json","w") as f:
        f.writelines(total_json)

if __name__ == '__main__':
    generate_las_json()
