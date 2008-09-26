class CreateExtracts < ActiveRecord::Migration
  def self.up
    create_table :extracts do |t|
      t.string :extract_type, :null => false, :default => 'Extract'
      t.string :status, :null => false, :default => 'Ready'
      t.string :options
      t.text :results
      t.string :results_file
      t.datetime :submitted_at
      t.datetime :completed_at
      t.timestamps
    end
  end
  
  def self.down
    drop_table :extracts
  end
end
