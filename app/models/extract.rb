class Extract < ActiveRecord::Base
  def self.inheritance_column
    'extract_type'
  end
  
  def title
    "#{extract_type} #{created_at.to_s(:long)}"
  end
    
  def results_basename
    results_file.blank? ? '' : File.basename(results_file)
  end
  
  def results_download_path
    results_file.blank? ? '/' : "/extracts/#{results_basename}"
  end
    
  def move_results_file(src_file)
    dst_dir = File.join(Rails.public_path, 'extracts')
    FileUtils.mkdir_p(dst_dir)
    dst_basename = File.basename(src_file).gsub(/[^-_\.a-zA-Z0-9]/, '_')
    dst_file = File.join(dst_dir, dst_basename)
    FileUtils.mv(src_file, dst_file, :force => true) 
    dst_file
  end  
    
  def run
    raise "run called on base class"
  end
end

