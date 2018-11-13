params.reads = "/data2/lxl/yangsheng/new_nextflow_falcon/work/rep-scripts/rep_*/run_daligner.sh"
params.combine = "/data2/lxl/yangsheng/new_nextflow_falcon/work/rep-combine/"
params.script_dir = "/data2/lxl/yangsheng/nextflow-falcon/script/"
params.las_json = "${params.script_dir}generate_las_json_from_txt.py"
params.script_new = "/data2/lxl/yangsheng/new_nextflow_falcon/script/"
params.generate_merge_jobs = "${params.script_new}generate_las_jobs.py"
params.run_merge = "${params.script_new}run_merge_las.py"
params.rep_split = "${params.script_new}build_rep_split.py"
params.run_rep_jobs= "${params.script_new}run_rep_jobs.py"
params.generate_rep_db="${params.script_new}generate_repeat_db.py"
params.build_daligner_jobs="${params.script_new}build_daligner_jobs.py"
params.get_daligner_las_json="${params.script_new}get_daligner_las_json.py"
params.merge_daligner_las_json="${params.script_new}merge_daligner_las_json.py"
params.merge_daligner_las="${params.script_new}merge_daligner_las.py"

params.daligner_opt = "-e.7 -l1000 -k18 -h80 -w8 -s100 -v -B128 -M24"

params.group_size=2
params.coverage=30
params.db = "/data2/lxl/yangsheng/new_nextflow_falcon/work/tan_db/raw_reads.db"


Channel
    .fromPath( params.reads,type: 'file' )
    .ifEmpty { error "Cannot find any reads matching: ${params.reads}" }
    .into { read_pairs_ch }

Channel
    .fromPath( "/data2/lxl/yangsheng/new_nextflow_falcon/work/tan_db/raw_reads.db",type: 'file' )
    .ifEmpty { error "Cannot find any reads matching: ${params.reads}" }
    .into { db_read }



Channel
    .fromPath( "/data2/lxl/yangsheng/new_nextflow_falcon/test/length_cutoff",type: 'file' )
    .ifEmpty { error "Cannot find any reads matching: ${params.reads}" }
    .set { length_cutoff }

process run_rep_daligner{
    maxForks 6
    input:
    file run_dali from read_pairs_ch
    output:
    file '*.las' into output
    """
        sh ${run_dali}
    """



}
process get_las_fn1 {
    input:
    val a from output.collect().toList()
    output:
    file "merge-scripts/m_*"  into merge_jobs mode flatten
    """
    echo $a > b.txt
    python ${params.las_json} b.txt
    python ${params.generate_merge_jobs} --las-json gather_las.json
    """
}

process run_merge4{
    cache 'lenient'
    maxForks 10
    input:
    file b from merge_jobs 
    output:
    file "m_*/Lraw_reads.*.las" into raw_las
    """
    cd ${b}
    python ${params.run_merge} --las-path las-paths.json --las-fn las_fn
    """
}
process get_las_json {
    input:
    val a from raw_las.collect().toList()
    output:
    file ("gather_las.json") into rep_las_json
    """
    echo $a > b.txt
    python ${params.las_json} b.txt
    """


}

process build_rep_task4{
    input:
    file las_fn from rep_las_json
    output:
    file "rep-scripts/rep_*/run_REPmask.sh" into rep_task1 mode flatten
    """
    python ${params.rep_split}  --las-json $las_fn --group-size ${params.group_size} --coverage ${params.coverage}
    """


}

process run_rep_task {
    //cache 'lenient'
    input:
    //val db1 from db_read
    file rep_job from rep_task1
    output:
    file "job.done" into rep_result 
    """
    python ${params.run_rep_jobs} --db-fn ${params.db} --script-fn run_REPmask.sh
    touch job.done
    """


}

process merge_rep_anno {
    input:
    val gather from  rep_result.collect()
    output:
    file "raw_reads.db" into rep_db  
    """
    echo $gather >gather.json
    python ${params.generate_rep_db} --db-fn ${params.db} --gather-fn gather.json --group-size 2   
    
    """
}



process generate_daligner_tasks2 {
    input:
    val db from rep_db 
    file length from length_cutoff
    output: 
    file "daligner-scripts/j_*/run_daligner.sh" into daligner_jobs mode flatten    
    """
    python ${params.build_daligner_jobs} --db-fn $db --daligner-opt "${params.daligner_opt} " --length-cutoff $length
    """

}


process run_daligner_jobs {
    cache 'lenient'
    maxForks 8
    input:
    file run_dal from daligner_jobs
    output:
    file "las_path.json" into single_daligner_las
    """
    sh $run_dal
    python ${params.get_daligner_las_json}
    """    

}

process merge_las_json1 {
    input:
    val las_file from single_daligner_las.collect()
    output:
    file "gather_las.json" into daligner_las_json   
    """
    echo $las_file > las_path
    python ${params.merge_daligner_las_json}
    """


}

process build_daligner_las_merge {
    input:
    file gather_las from daligner_las_json
    output:
    
    """
    python ${params.merge_daligner_las} --las-fn $gather_las
    """



}
