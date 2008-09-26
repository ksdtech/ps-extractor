class DataDirectorExtract < Extract
  def run
    raise "cannot resubmit extract" if status != 'Ready'
    update_attributes(:status => 'Started',
      :submitted_at => Time.now)
    dd = DataDirectorExporter.new('08-09', '/tmp/extracts', 
      File.join(Rails.root, 'config/ddexport.yml'))
    dd.run_powerschool_queries
    if dd.process_files
      dst_file = move_results_file(dd.zip_file_path)
      update_attributes(:status => 'Complete', 
        :results => dd.results,
        :results_file => dst_file, 
        :completed_at => Time.now)
    else
      update_attributes(:status => 'Failed', 
        :results => dd.results,
        :completed_at => Time.now)
    end
  end
end
