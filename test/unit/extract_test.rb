require 'test_helper'

class ExtractTest < ActiveSupport::TestCase
  def test_should_be_valid
    assert Extract.new.valid?
  end
end
