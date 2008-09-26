class ExtractsWorker < Workling::Base
  def do_extract(options)
    extract = Extract.find(options[:extract_id])
    extract.run
  end
end