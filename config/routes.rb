ActionController::Routing::Routes.draw do |map|
  map.resources :extracts, :member => { :submit => :post }
  map.resources :data_director_extracts, :controller => 'extracts', :member => { :submit => :post }
  map.root :extracts
end
