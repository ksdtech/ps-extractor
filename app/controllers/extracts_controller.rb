class ExtractsController < ApplicationController
  def submit
    ExtractsWorker.async_do_extract(:extract_id => params[:id])
    flash[:notice] = "Extract submitted"
    redirect_to extracts_url
  end
  
  def index
    @extracts = Extract.find(:all)
  end
  
  def show
    @extract = Extract.find(params[:id])
  end
  
  def new
    @extract = Extract.new
  end
  
  def create
    extract_type = params[:extract].delete(:extract_type)
    @extract = case 
      when 'DataDirectorExtract'
        DataDirectorExtract.new(params[:extract])
      else
        Extract.new(params[:extract])
      end
    if @extract.save
      flash[:notice] = "Successfully created extract."
      redirect_to @extract
    else
      render :action => 'new'
    end
  end
  
  def edit
    @extract = Extract.find(params[:id])
  end
  
  def update
    @extract = Extract.find(params[:id])
    if @extract.update_attributes(params[:extract])
      flash[:notice] = "Successfully updated extract."
      redirect_to @extract
    else
      render :action => 'edit'
    end
  end
  
  def destroy
    @extract = Extract.find(params[:id])
    @extract.destroy
    flash[:notice] = "Successfully destroyed extract."
    redirect_to extracts_url
  end
end
