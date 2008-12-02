require 'rubygems'
require 'faster_csv'

class GpaCalculator
  GPA_COURSES = %w{
    5000
    5100
    5108
    5200
    5400
    5600
    6100
    6101
    6102
    6103
    6104
    6108
    6109
    6200
    6201
    6202
    6203
    6204
    6205
    6206
    6400
    6600
    6601
    6602
    6603
    6604
    6605
    6606
    7100
    7101
    7103
    7104
    7108
    7128
    7129
    7200
    7400
    7600
    7800
    7805
    7808
    8100
    8102
    8103
    8104
    8105
    8107
    8108
    8116
    8128
    8129
    8200
    8400
    8600
    8800
    8801
    8802
    8805
    8808
    6102b
    6103b
    6104b
    6400b
    7101b
    7103b
    7104b
    7200b
    7400b
    8100b
    8103b
    8104b
    8128b
    8129b
    8200b
    8400b
    8600b
  }

  GPA_CREDIT_TYPES = ['la', 'math', 'history', 'science', 'spanish']

  def initialize(all_courses=true)
    @courses = { }
    @students = { }
    @years = { }
    @gpas = { }
    @all_courses = all_courses
  end
  
  def academic_gpa_course?(course_number, credit_type)
    return true if !credit_type.nil? && !credit_type.empty? &&
      GPA_CREDIT_TYPES.include?(credit_type.downcase)
    return true if GPA_COURSES.include?(course_number)
  end
  
  def valid_codes(year_abbr)
    year_abbr < '07-08' ? ['Q1', 'Q2', 'Q3', 'Q4'] : ['T1', 'T2', 'T3']
  end
  
  def academic_store_code?(year_abbr, store_code)
    valid_codes(year_abbr).include?(store_code)
  end

  def process_csv(path, &block)
    FasterCSV.open(path, :col_sep => "\t", :row_sep => "\n", :headers => true) do |csv|
      csv.header_convert { |h| h.downcase.tr(" ", "_").delete("^a-z_").to_sym }
      csv.each(&block)
    end
  end
  
  def term_to_year_abbr(termid)
    year_number_to_year_abbr(termid.to_i/100)
  end
  
  def year_number_to_year_abbr(year_number)
    sprintf("%02d-%02d", (year_number + 90) % 100, (year_number + 91) % 100)
  end
  
  def store_grade(bucket, year_abbr, student_number, term_id, store_code, section_id, points)
    @years[year_abbr] = true
    student_year = "#{year_abbr}:#{student_number}"
    if academic_store_code?(year_abbr, store_code)
      key = "#{store_code} #{bucket}"
      @gpas[student_year][key][:raw].push([points, section_id, term_id, store_code])
    end
    key = "Cumulative #{bucket}"
    @gpas[student_year][key][:raw].push([points, section_id, term_id, store_code])
  end
  
  def sum_grades
    @gpas.each do |student_year, student_bucket|
      student_bucket.each do |gpa_name, gpa_bucket|
        gpa_count = 0
        gpa_points = 0.0
        gpa_bucket[:raw].each do |row|
          gpa_count += 1
          gpa_points += row.first
        end
        gpa_points = (gpa_points / gpa_count) if gpa_count != 0
        gpa_bucket[:avg] = sprintf("%.2f", gpa_points)
      end
    end
  end
  
  def report_gpas(f, year_abbr)
    store_codes = valid_codes(year_abbr) + ['Cumulative']
    headers = ['sssid', 'localid', 'last_name', 'first_name']
    ['Simple', 'Academic'].each do |bucket|
      store_codes.each do |store_code|
        headers.push("#{store_code} #{bucket} GPA")
      end
    end
    f.write headers.join("\t")
    f.write "\n"
    @gpas.each do |student_year, student_bucket|
      the_year, student_number = student_year.split(':')
      next unless year_abbr == the_year
      values = @students[student_number]
      next if values.nil?
      ['Simple', 'Academic'].each do |bucket|
        store_codes.each do |store_code|
          key = "#{store_code} #{bucket}"
          values.push(student_bucket[key][:avg]) if !student_bucket[key].nil?
        end
      end
      f.write values.join("\t")
      f.write "\n"
    end
  end

  def init_student_year_buckets(year_abbr, student_number)
    student_year = "#{year_abbr}:#{student_number}"
    return unless @gpas[student_year].nil?
    
    @gpas[student_year] = { }
    ['Simple', 'Academic'].each do |bucket|
      store_codes = valid_codes(year_abbr) + ['Cumulative']
      store_codes.each do |store_code|
        key = "#{store_code} #{bucket}"
        @gpas[student_year][key] ||= { :raw => [ ], :avg => 0.0 }
      end
    end
  end

  def calculate_gpas(only_year='07-08')
    row_num = 0
    process_csv('stored_grades.txt') do |row|
      begin
        row_num += 1
        $stderr.print "scanning grades: row #{row_num}\n" if (row_num % 100) == 1
        term_id       = row[:termid]
        year_abbr = term_to_year_abbr(term_id)
        next if !only_year.nil? && year_abbr != only_year
        # p row.to_hash
        course_number  = row[:course_number]
        @courses[course_number] = row[:course_name]
        exclude       = (row[:excludefromgpa] || 0).to_i != 0
        grade         = row[:grade]
        if !exclude && !grade.nil? && !grade.empty?
          student_number = row[:student_number]
          @students[student_number] = [ row[:state_studentnumber], student_number, row[:last_name], row[:first_name], ]
          section_id     = row[:sectionid]
          percent        = row[:percent]
          points         = (row[:gpa_points] || 0).to_f
          grade_level    = row[:grade_level]
          store_date     = row[:datestored]
          credit_type    = row[:credit_type]
          store_code     = row[:storecode]
          init_student_year_buckets(year_abbr, student_number)
        
          if academic_store_code?(year_abbr, store_code) && 
            academic_gpa_course?(course_number, credit_type)
            store_grade('Academic', year_abbr, student_number, term_id, store_code, section_id, points)
          end
          store_grade('Simple', year_abbr, student_number, term_id, store_code, section_id, points)
        end
      rescue
        print "error on row #{row_num}\n"
        p row.to_hash
        raise
      end
    end
    
    sum_grades
  end
  
  def export_gpas(fname)
    @years.keys.each do |year_abbr|
      File.open("#{year_abbr}#{fname}", 'w') do |f|
        report_gpas(f, year_abbr)
      end
    end
  end
end

gc = GpaCalculator.new
gc.calculate_gpas('04-05')
gc.export_gpas('gpas.txt')
