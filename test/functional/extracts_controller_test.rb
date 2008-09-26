require 'test_helper'

class ExtractsControllerTest < ActionController::TestCase
  def test_index
    get :index
    assert_template 'index'
  end
  
  def test_show
    get :show, :id => Extract.first
    assert_template 'show'
  end
  
  def test_new
    get :new
    assert_template 'new'
  end
  
  def test_create_invalid
    Extract.any_instance.stubs(:valid?).returns(false)
    post :create
    assert_template 'new'
  end
  
  def test_create_valid
    Extract.any_instance.stubs(:valid?).returns(true)
    post :create
    assert_redirected_to extract_url(assigns(:extract))
  end
  
  def test_edit
    get :edit, :id => Extract.first
    assert_template 'edit'
  end
  
  def test_update_invalid
    Extract.any_instance.stubs(:valid?).returns(false)
    put :update, :id => Extract.first
    assert_template 'edit'
  end
  
  def test_update_valid
    Extract.any_instance.stubs(:valid?).returns(true)
    put :update, :id => Extract.first
    assert_redirected_to extract_url(assigns(:extract))
  end
  
  def test_destroy
    extract = Extract.first
    delete :destroy, :id => extract
    assert_redirected_to extracts_url
    assert !Extract.exists?(extract.id)
  end
end
