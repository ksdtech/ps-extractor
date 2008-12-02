require 'rubygems'
require 'yaml'
require 'faster_csv'
require 'ftools'
require 'dbi'

# see export_file_formats.txt for information on required input files
# teachers.txt
# courses.txt
# students.txt
# reenrollments.txt
# cc.txt

class Hash
  def symbolize_keys_r
    inject({}) do |options, (key, value)|
      value.replace(value.symbolize_keys_r) if value.is_a?(Hash)
      options[(key.to_sym rescue key) || key] = value
      options
    end
  end  
  
  def symbolize_keys_r!
    self.replace(self.symbolize_keys_r)
    self
  end
end

class DataDirectorExporter
  
  ### NOTE: the field numbers for custom fields will be different
  ### in your version of PowerSchool.  You can look them up using
  ### the 'Fields' or 'FieldsTable' field.   The ID number of
  ### the record corresponding to the field Name is what you want.

  # sybase -> convert(char(10), st.SchoolEntryDate, 101) AS SchoolEntryDate
  # oracle -> TO_CHAR(st.SchoolEntryDate, MM/DD/YYYY') AS SchoolEntryDate


  # sybase -> convert(int, st.Student_Number) AS Student_Number
  # oracle -> TO_NUMBER(st.Student_Number) AS Student_Number

  QUERIES = {
  :student_query => %{ SELECT st.ID,
  st.State_StudentNumber,
  TO_NUMBER(st.Student_Number) AS Student_Number,
  st.SchoolID,
  st.First_Name,
  st.Last_Name,
  TO_CHAR(st.DOB, 'MM/DD/YYYY') AS DOB,
  st.Ethnicity,
  st.Gender,
  st.Enroll_Status,
  st.Grade_Level,
  mf.Value AS Mother_First,
  st.Mother,
  ff.Value AS Father_First,
  st.Father,
  st.Street,
  st.City,
  st.State,
  st.Zip,
  st.Home_Phone,
  TO_CHAR(st.SchoolEntryDate, 'MM/DD/YYYY') AS SchoolEntryDate,
  TO_CHAR(st.DistrictEntryDate, 'MM/DD/YYYY') AS DistrictEntryDate,
  TO_CHAR(st.EntryDate, 'MM/DD/YYYY') AS EntryDate,
  TO_CHAR(st.ExitDate, 'MM/DD/YYYY') AS ExitDate,
  sch.Alternate_School_Number,
  hl.Value AS CA_HomeLanguage,
  lf.Value AS CA_LangFluency,
  fs.Value AS CA_FirstUSASchooling,
  pd.Value AS CA_PrimDisability,
  pe.Value AS CA_ParentEd,
  rfep.Value AS CA_DateRFEP
  FROM Students st
  LEFT OUTER JOIN Schools sch ON sch.School_Number=st.SchoolID
  LEFT OUTER JOIN CustomText mf ON (mf.FieldNo={{Mother_First}} AND mf.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText ff ON (ff.FieldNo={{Father_First}} AND ff.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText hl ON (hl.FieldNo={{CA_HomeLanguage}} AND hl.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText lf ON (lf.FieldNo={{CA_LangFluency}} AND lf.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText fs ON (fs.FieldNo={{CA_FirstUSASchooling}} AND fs.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText pd ON (pd.FieldNo={{CA_PrimDisability}} AND pd.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText pe ON (pe.FieldNo={{CA_ParentEd}} AND pe.KeyNo=st.DCID)
  LEFT OUTER JOIN CustomText rfep ON (rfep.FieldNo={{CA_DateRFEP}} AND rfep.KeyNo=st.DCID) }, 
  
  :teacher_query => %{ 
  SELECT t.ID, t.TeacherNumber, t.SchoolID, 
  sch.Alternate_School_Number, 
  t.First_Name, t.Last_Name, t.Email_Addr,
  t.Status, t.StaffStatus 
  FROM Teachers t
  LEFT OUTER JOIN Schools sch ON sch.School_Number=t.schoolID },
  
  :school_query => %{ SELECT sch.Name, sch.School_Number,
  sch.Low_Grade, sch.High_Grade, sch.Alternate_School_Number
  FROM Schools sch }, 
  
  :course_query => %{ SELECT c.Course_Number, c.Course_Name, c.Credit_Hours, 
  c.CreditType, cl.Value AS CA_CourseLevel, 
  c.SchoolID, sch.Alternate_School_Number
  FROM Courses c 
  LEFT OUTER JOIN Schools sch ON sch.School_Number=c.SchoolID 
  LEFT OUTER JOIN CustomText cl ON (cl.FieldNo={{CA_CourseLevel:300}} AND cl.KeyNo=c.ID) },
  
  :roster_query => %{ Select cc.TermID, st.State_StudentNumber, cc.StudentID, 
  f.TeacherNumber, cc.TeacherID, cc.SchoolID, 
  sch.Alternate_School_Number, st.Grade_Level, 
  cc.Expression, t.Abbreviation, 
  cc.Course_Number, cc.Section_Number, cc.SectionID 
  FROM cc
  LEFT OUTER JOIN Students st ON st.ID=cc.StudentID 
  LEFT OUTER JOIN Teachers f ON f.ID=cc.TeacherID 
  LEFT OUTER JOIN Schools sch ON sch.School_Number=cc.SchoolID 
  LEFT OUTER JOIN Terms t ON (t.ID=ABS(cc.TermID)) },

  :roster_query_reenrollments => %{ Select cc.TermID, st.State_StudentNumber, cc.StudentID, 
  f.TeacherNumber, cc.TeacherID, cc.SchoolID, 
  sch.Alternate_School_Number, re.Grade_Level, 
  cc.Expression, t.Abbreviation, 
  cc.Course_Number, cc.Section_Number, cc.SectionID 
  FROM cc
  LEFT OUTER JOIN Students st ON st.ID=cc.StudentID 
  LEFT OUTER JOIN Teachers f ON f.ID=cc.TeacherID 
  LEFT OUTER JOIN Schools sch ON sch.School_Number=cc.SchoolID 
  LEFT OUTER JOIN Terms t ON (t.ID=ABS(cc.TermID))
  LEFT OUTER JOIN ReEnrollments re ON (re.StudentID=st.ID AND re.EntryDate<=cc.DateEnrolled AND re.ExitDate>=cc.DateLeft) },

  :reenrollment_query => %{ SELECT st.State_StudentNumber, re.StudentID, re.SchoolID,
  sch.Alternate_School_Number, re.Grade_Level, 
  TO_CHAR(re.EntryDate, 'MM/DD/YYYY') AS EntryDate, 
  TO_CHAR(re.ExitDate, 'MM/DD/YYYY') AS ExitDate
  FROM ReEnrollments re
  LEFT OUTER JOIN Students st ON st.ID=re.StudentID
  LEFT OUTER JOIN Schools sch ON sch.School_Number=re.SchoolID } 

  }
  
  TERM_ABBRS = {
    '01-02' => 'Y',
    '02-03' => 'Y',
    '03-04' => 'Y',
    '04-05' => 'Y',
    '05-06' => 'Y',
    '06-07' => 'Y',
    '07-08' => 'Y',
    '08-09' => 'Y',
    'HT 1' => 'H1',
    'HT 2' => 'H2',
    'HT 3' => 'H3',
    'HT 4' => 'H4',
    'HT 5' => 'H5',
    'HT 6' => 'H6',
    'HT1' => 'H1',
    'HT2' => 'H2',
    'HT3' => 'H3',
    'HT4' => 'H4',
    'HT5' => 'H5',
    'HT6' => 'H6',
  }
  
  def initialize(year_only=nil, root_d=nil, conf_file='ddexport.yml')
    root_d = File.dirname(__FILE__) if root_d.nil?
    conf_file = File.join(root_d, '../config', conf_file) unless conf_file[0,1] == '/'
    @year_only = year_only
    @dbh = nil
    @config = YAML.load_file(conf_file)
    @config.symbolize_keys_r!
    @rosters = { }
    @users = { }
    @courses = { }
    @students = { }
    @enrollments = { }
    @teacher_years = { }
    @log_lines = [ ]
    @custom_fields = { }
    @zip_name = "kentfield-#{Time.now.strftime("%Y%m%d")}"
    @root_dir = File.expand_path(root_d)
    @input_dir = File.join(@root_dir, 'psexport')
    @output_dir = File.join(@root_dir, @zip_name)
  end
  
  def log(line)
    @log_lines << line
  end

  def results
    @log_lines.join("\n")
  end

  def zip_file_path
    "#{@output_dir}.zip"
  end
  
  def custom_field_number(field)
    field, fileno = field.split(/:/)
    fileno = 100 unless fileno
    key = "#{field.downcase}:#{fileno}"
    if !@custom_fields.key?(field)
      field_id = 0
      sql = "SELECT ID FROM FieldsTable WHERE FileNo=#{fileno} AND REGEXP_LIKE(Name,'#{field}','i')"
      row = @dbh.select_one(sql)
      field_id = row[0].to_i if row
      puts "#{key} -> #{field_id}"
      @custom_fields[key] = field_id
    end
    @custom_fields[key]
  end
  
  def run_query(query_name, fname, min_termid=nil)
    log "processing #{query_name}..."
    num_rows = 0
    sql = QUERIES[query_name].gsub(/\{\{([^}]+)\}\}/) do |field|
      custom_field_number($1)
    end
    if query_name == :roster_query && !min_termid.nil?
      sql << " WHERE ABS(cc.TermID)>=#{min_termid}"
    end
    begin
      puts "executing #{sql}"
      sth = @dbh.execute(sql)
      f = nil
  	  while row = sth.fetch do
  	    if f.nil?
  	      f = File.open(fname, "w")
          f.write row.column_names.join("\t")
          f.write "\n"
  	    end
        vals = [ ] 
        row.each_with_name do |val, name|
          val = val.to_i if val.is_a?(BigDecimal)
          vals << val
        end
  	    f.write vals.join("\t")
  	    f.write "\n"
  	    num_rows += 1
  	  end
      log " #{num_rows} rows written to #{fname}"
  	  sth.finish
  	rescue DBI::DatabaseError => e
  	  log " query error: #{$!}"
  	rescue
  	  raise
  	end
  end
  
  def connect_db
    return if @dbh
    dsn = "dbi:#{@config[:src_db][:adapter]}:#{@config[:src_db][:database]}"
	  begin
	    @dbh = DBI.connect(dsn, @config[:src_db][:user], @config[:src_db][:password])
	    puts "connection open"
	  rescue
	    log "could not open #{dsn}: #{$!}"
	    puts results
	    exit
	  end
  end
  
  def disconnect_db
    return unless @dbh
  end
  
  def run_powerschool_queries
    connect_db
    if @dbh
      min_termid = @year_only.nil? ? nil : year_abbr_to_term(@year_only)
      File.makedirs(@input_dir) if !File.directory?(@input_dir)

      run_query(:school_query,  "#{@input_dir}/schools.txt")
      run_query(:course_query,  "#{@input_dir}/courses.txt")
      run_query(:student_query, "#{@input_dir}/students.txt")
      run_query(:reenrollment_query, "#{@input_dir}/reenrollments.txt")
      run_query(:teacher_query, "#{@input_dir}/teachers.txt")
      run_query(:roster_query,  "#{@input_dir}/cc.txt", min_termid)
      
      disconnect_db
      puts results
      exit
    end
  end
  
  def process_csv(path, &block)
    FasterCSV.open(path, :col_sep => "\t", :row_sep => "\n", :headers => true) do |csv|
      csv.header_convert { |h| h.downcase.tr(" ", "_").delete("^a-z_").to_sym }
      csv.each(&block)
    end
  end

  def analyze_student_data(year)
    process_csv("#{@input_dir}/students.txt") do |row|
      studentid = row[:id]
      parent_name = "#{row[:mother_first]} #{row[:mother]}".strip
      parent_name = "#{row[:father_first]} #{row[:father]}".strip if parent_name.empty?    
      disability = row[:ca_primdisability]
      disability = '' if disability.to_i == 0
      special_program = disability.empty? ? '' : 'rsp'

      set_student(studentid, :ssid,       row[:state_studentnumber])
      set_student(studentid, :student_id, row[:student_number])
      set_student(studentid, :first_name, row[:first_name])
      set_student(studentid, :last_name,  row[:last_name])
      set_student(studentid, :birthdate,  row[:dob])
      set_student(studentid, :gender,     row[:gender])
      set_student(studentid, :parent,     parent_name)
      set_student(studentid, :street,     row[:street])
      set_student(studentid, :city,       row[:city])
      set_student(studentid, :state,      row[:state])
      set_student(studentid, :zip,        row[:zip])
      set_student(studentid, :phone_number,          row[:home_phone])
      set_student(studentid, :primary_language,      row[:ca_homelanguage])
      set_student(studentid, :ethnicity,             row[:ethnicity])
      set_student(studentid, :language_fluency,      row[:ca_langfluency])
      set_student(studentid, :date_entered_school,   row[:schoolentrydate])
      set_student(studentid, :date_entered_district, row[:districtentrydate])
      set_student(studentid, :first_us_entry_date,   row[:ca_firstusaschooling])
      set_student(studentid, :gate,       '')
      set_student(studentid, :primary_disability,    disability)
      set_student(studentid, :nslp,       '')
      set_student(studentid, :parent_education,      row[:ca_parented])
      set_student(studentid, :migrant_ed, '')
      set_student(studentid, :date_rfep,             row[:ca_daterfep])
      set_student(studentid, :special_program,       special_program)
      set_student(studentid, :title_1,    '')

      enroll_status = row[:enroll_status].to_i
      enroll_year = date_to_year_abbr(row[:entrydate])
      if enroll_status == 0 || (year == enroll_year && enroll_status > 0)
        set_enrollment(year, studentid, :school_id,   row[:schoolid])
        set_enrollment(year, studentid, :school_code, row[:alternate_school_number])
        set_enrollment(year, studentid, :grade_level, row[:grade_level])
      end
    end

    if File.file? "#{@input_dir}/reenrollments.txt"
      process_csv("#{@input_dir}/reenrollments.txt") do |row|
        year = date_to_year_abbr(row[:entrydate])
        studentid = row[:studentid]
        set_enrollment(year, studentid, :school_id,   row[:schoolid])
        set_enrollment(year, studentid, :school_code, row[:alternate_school_number])
        set_enrollment(year, studentid, :grade_level, row[:grade_level])
      end
    end
  end
  
  def analyze_user_data(year)
    process_csv("#{@input_dir}/teachers.txt") do |row|
      userid = row[:id]
      teacherid = row[:teachernumber]
      
      set_user(userid,  :employee_id,   teacherid)
      set_user(userid,  :teacher_id,    teacherid)
      set_user(userid,  :school_id,     row[:schoolid])
      set_user(userid,  :school_code,   row[:alternate_school_number])
      set_user(userid,  :first_name,    row[:first_name])
      set_user(userid,  :last_name,     row[:last_name])
      set_user(userid,  :email_address, row[:email_addr])
      
      # current teachers or specified administrators
      if row[:status].to_i == 1 && 
        (row[:datadirector_access].to_i == 1 || row[:staffstatus].to_i == 1)
        set_teacher_year(year, userid, :active, 'y')
      end
    end
  end
  
  def analyze_course_data
    process_csv("#{@input_dir}/courses.txt") do |row|
      courseid = row[:course_number]
      abbreviation = course_abbreviation(row[:course_name])
      set_course(courseid, :course_id,    courseid)
      set_course(courseid, :abbreviation, abbreviation)
      set_course(courseid, :name,         row[:course_name])
      set_course(courseid, :credits,      row[:credit_hours])
      set_course(courseid, :subject_code, row[:credittype])
      set_course(courseid, :a_to_g,       '')
      set_course(courseid, :school_id,    row[:schoolid])
      set_course(courseid, :school_code,  row[:alternate_school_number])
    end
  end
  
  def analyze_cc_data
    excluded_courses = [ 'AAAA', 'oooo' ]  # Attendance
    non_excluded_courses = [
      '0500', '1500', '2500', '3500', '4500', # Bacich Library
      '0820', '1820', '2820', '3820', '4820', # Bacich Art
      '0830', '1830', '2830', '3830', '4830', # Bacich Tech
      '0880', '1880', '2880', '3880', '4880', # Bacich Musicß
      '0881', '1881', '2881', '3881', '4881', # Bacich Chorus
      '0700', '1700', '2700', '3700', '4700', # Bacich PE
      ]
    
    process_csv("#{@input_dir}/cc.txt") do |row|
      courseid  = row[:course_number]
      next if excluded_courses.include?(courseid)
      
      studentid = row[:studentid]
      userid    = row[:teacherid]
      sectionid = row[:sectionid]
      next if sectionid.nil?
      sectionid.gsub!(/^[-]/, '')
      period = expression_to_period(row[:expression])
      next if period.nil?
      term   = term_abbreviation(row[:abbreviation])
      next if term.nil?
      memberid = "#{courseid}-#{studentid}"
      year = term_to_year_abbr(row[:termid].gsub(/^[-]/, ''))
      
      set_teacher_year(year, userid, :active, 'y')
      
      set_roster(year, memberid, :ssid,        student(studentid, :ssid))
      set_roster(year, memberid, :student_id,  student(studentid, :student_id))
      set_roster(year, memberid, :teacher_id,  user(userid, :teacher_id))
      set_roster(year, memberid, :employee_id, user(userid, :employee_id))
      set_roster(year, memberid, :school_id,   row[:schoolid])
      set_roster(year, memberid, :school_code, row[:alternate_school_number])
      set_roster(year, memberid, :grade_level, enrollment(year, studentid, :grade_level))
      set_roster(year, memberid, :period,      period)
      set_roster(year, memberid, :term,        term)
      set_roster(year, memberid, :course_id,   courseid)
      set_roster(year, memberid, :section_id,  sectionid)
    end
  end
  
  def output_files
    roster_fields = [
      :ssid, :student_id, :teacher_id, :employee_id, 
      :school_id, :school_code, :grade_level, :period, :term, :course_id, :section_id ]
      
    course_keys = { }
        
    File.makedirs(@output_dir) if !File.directory?(@output_dir)
    
    years = @rosters.keys.sort { |a,b| b <=> a }
    years.each do |year|
      fname = "#{year}rosters.txt"
      log "opening #{fname}"
      lines = 0
      File.open("#{@output_dir}/#{fname}", 'w') do |out|
        header_fields = roster_fields.collect { |f| f.to_s }.join("\t")
        out.write("#{header_fields}\n")
        members = @rosters[year].keys.sort
        members.each do |memberid|
          # mark courses
          courseid = roster(year, memberid, :course_id)
          course_keys[courseid] = 1
          values = roster_fields.collect { |f| roster(year, memberid, f) }.join("\t")
          out.write("#{values}\n")
          lines += 1
        end
      end
      log " -> #{lines} records written"
      
      user_fields = [ :employee_id, :teacher_id, :school_id, :school_code, 
        :first_name, :last_name, :email_address ]
      fname = "#{year}users.txt"
      log "opening #{fname}"
      lines = 0
      File.open("#{@output_dir}/#{fname}", 'w') do |out|
        header_fields = user_fields.collect { |f| f.to_s }.join("\t")
        out.write("#{header_fields}\n")
        teachers = @teacher_years[year].keys
        teachers.each do |userid|
          next if user(userid, :school_code) == 0
          values = user_fields.collect { |f| user(userid, f) }.join("\t")
          out.write("#{values}\n")
          lines += 1
        end
      end
      log " -> #{lines} records written"

      demo_fields = [ :ssid, :student_id, :school_code, :first_name, :last_name, 
        :birthdate, :gender, :parent, :street, :city, :state,  :zip, :phone_number,
        :primary_language, :ethnicity, :language_fluency,
        :date_entered_school, :date_entered_district, :first_us_entry_date,
        :gate, :primary_disability, :nslp, :parent_education, :migrant_ed,
        :date_rfep, :special_program, :title_1 ]
        
      fname = "#{year}demo.txt"
      log "opening #{fname}"
      lines = 0
      File.open("#{@output_dir}/#{fname}", 'w') do |out|
        header_fields = demo_fields.collect { |f| f.to_s }.join("\t")
        out.write("#{header_fields}\n")
        students = @enrollments[year].keys
        students.each do |studentid|
          ssid = student(studentid, :ssid)
          next if ssid.nil? || ssid.empty?
          set_student(studentid, :school_id,   enrollment(year, studentid, :school_id))
          set_student(studentid, :school_code, enrollment(year, studentid, :school_code))
          values = demo_fields.collect { |f| student(studentid, f) }.join("\t")
          out.write("#{values}\n")
          lines += 1
        end
      end
      log " -> #{lines} records written"
      
      system("zip -j -r #{@output_dir} #{@output_dir}")
      true
    end
    
    # note: can we do subject mapping?
    course_fields = [ :course_id, :abbreviation, :name,
      :credits, :subject_code, :a_to_g, :school_id, :school_code ]
    fname = "all-courses.txt"
    log "opening #{fname}"
    lines = 0
    File.open("#{@output_dir}/#{fname}", 'w') do |out|
      header_fields = course_fields.collect { |f| f.to_s }.join("\t")
      out.write("#{header_fields}\n")
      course_keys.each_key do |courseid|
        values = course_fields.collect { |f| course(courseid, f) }.join("\t")
        out.write("#{values}\n")
        lines += 1
      end
    end
    log " -> #{lines} records written"
  end
  
  def set_course(courseid, key, value)
    (@courses[courseid] ||= { })[key] = value
  end
  
  def course(courseid, key)
    return nil unless @courses.has_key?(courseid)
    @courses[courseid][key]
  end

  def set_user(userid, key, value)
    (@users[userid] ||= { })[key] = value
  end
  
  def user(userid, key)
    return nil unless @users.has_key?(userid)
    @users[userid][key]
  end
  
  def set_student(studentid, key, value)
    (@students[studentid] ||= { })[key] = value
  end
  
  def student(studentid, key)
    return nil unless @students.has_key?(studentid)
    @students[studentid][key]
  end
  
  def set_enrollment(year, studentid, key, value)
    ((@enrollments[year] ||= { })[studentid] ||= { })[key] = value
  end
  
  def enrollment(year, studentid, key)
    return nil unless @enrollments.has_key?(year)
    return nil unless @enrollments[year].has_key?(studentid)
    @enrollments[year][studentid][key]
  end
  
  def set_teacher_year(year, userid, key, value)
    ((@teacher_years[year] ||= { })[userid] ||= { })[key] = value
  end
  
  def teacher_year(year, userid, key)
    return nil unless @teacher_years.has_key?(year)
    return nil unless @teacher_years[year].has_key?(userid)
    @teacher_years[year][userid][key]
  end

  def set_roster(year, memberid, key, value)
    ((@rosters[year] ||= { })[memberid] ||= { })[key] = value
  end
  
  def roster(year, memberid, key)
    return nil unless @rosters.has_key?(year)
    return nil unless @rosters[year].has_key?(memberid)
    @rosters[year][memberid][key]
  end
  
  def process_for_single_year
    analyze_course_data
    analyze_user_data(@year_only)
    analyze_student_data(@year_only)
    analyze_cc_data
    output_files
  end
  
  def process_for_all_years
    puts "analyzing all courses"
    analyze_course_data
    [ '01-02', '02-03', '03-04', '04-05', '05-06', '06-07', '07-08', '08-09' ].each do |year|
      puts "analyzing users for #{year}"
      analyze_user_data(year)
      puts "analyzing students for #{year}"
      analyze_student_data(year)
    end
    puts "analyzing all year enrollments"
    analyze_cc_data
    puts "writing files"
    output_files
  end
  
  def process_files
    if @year_only.nil?
      process_for_all_years
    else
      process_for_single_year
    end
  end
  
  def expression_to_period(expr)
    expr.nil? ? nil : expr.gsub(/[^0-9].*$/, '').to_i
  end
  
  def term_abbreviation(term_abbr)
    TERM_ABBRS[term_abbr] || term_abbr
  end
  
  def course_abbreviation(name)
    words = name.split
    abbr = words.first[0, 4].upcase.strip
    suffix = (words.size > 1 && (words.last == 'K' || words.last.to_i != 0)) ?
      words.last.upcase : ''
    "#{abbr}#{suffix}"
  end
  
  def parse_date(s)
    m, d, y = s.split(/[-\/]/).collect { |part| part.strip.empty? ? nil : part.to_i }
    if y.nil?
      # no year specified
      if d.nil?
        # raise "unrecognized date format: #{s}"
        # assume y: convert to 1/1/y
        y = m
        m = 1
        d = 1
      else
        # assume m/y: convert to m/1/y
        y = d
        d = 1
      end
    end
    if !m.nil? && m > 1900 
      # assume y/m/d
      t = y
      y = m
      m = d
      d = t
    end
    raise "invalid month" if m.nil? || m < 1 || m > 12
    raise "invalid day" if d.nil? || d < 1 || d > 31
    if !y.nil?
      if y < 20
        y += 2000
      elsif y < 100
        y += 1900
      end
    end
    raise "invalid year" if y.nil? || y < 1940 || y > 2015
    return Date.new(y, m, d)
  end
  
  def date_to_year_abbr(entrydate)
    entrydate = parse_date(entrydate)
    year_number = entrydate.month >= 7 ? entrydate.year-1990 : entrydate.year-1991
    year_number_to_year_abbr(year_number)
  end
  
  def year_abbr_to_term(year)
    sprintf("%02d00", (year.split('-')[0].to_i + 10) % 100)
  end
  
  def term_to_year_abbr(termid)
    year_number_to_year_abbr(termid.to_i/100)
  end
  
  def year_number_to_year_abbr(year_number)
    sprintf("%02d-%02d", (year_number + 90) % 100, (year_number + 91) % 100)
  end
end

dde = DataDirectorExporter.new('08-09')
#dde.run_powerschool_queries
dde.process_files


