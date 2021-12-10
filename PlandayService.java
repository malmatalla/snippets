package com.pawsec.kitchen.service;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pawsec.kitchen.dao.PlandayDAO;
import com.pawsec.kitchen.model.planday.Auth;
import com.pawsec.kitchen.model.planday.AuthJSON;
import com.pawsec.kitchen.model.planday.BreakJSON;
import com.pawsec.kitchen.model.planday.Department;
import com.pawsec.kitchen.model.planday.DepartmentJSON;
import com.pawsec.kitchen.model.planday.EmployeeJSON;
import com.pawsec.kitchen.model.planday.EmployeeMissedPunchouts;
import com.pawsec.kitchen.model.planday.MissedPunchouts;
import com.pawsec.kitchen.model.planday.PlandayConstants;
import com.pawsec.kitchen.model.planday.PunchClockShift;
import com.pawsec.kitchen.model.planday.PunchClockShiftJSON;
import com.pawsec.kitchen.model.planday.WorkedHoursAndMissedPunchouts;
import com.pawsec.kitchen.model.planday.WorkedHoursPerHour;

@Component
public class PlandayService {
	@Autowired
	PlandayDAO plandayDAO; 
	
	private static final Logger logger = LoggerFactory.getLogger(PlandayService.class);	
	
	private DepartmentJSON departmentsRecursingJSON; 
	private PunchClockShiftJSON punchClockShiftOutputRecursingJSON; 
	private int recursingOffsetCount = 50; 
	// private BreakJSON breaksRecursingWrapper; 
	
	public Auth getPlandayAuth() {
		logger.debug("getPlandayAuth");
		Auth auth = plandayDAO.getPlandayAuth();
		Date now = new Date(); 
		//TODO Handle NullPointerException and EmptyResultDataSetException
		if(auth != null) {
			long expirationTimeInMillis = auth.getUpdatedDate().getTime().getTime() + (auth.getExpires() * 1000); 
			if(now.getTime() > expirationTimeInMillis) {
				logger.debug("Authentication tokens was expired. Refresing information.");
				auth = refreshPlandayAuth(); 
			}else {
				logger.debug("Authentication tokens are still valid.");
			}			
		}

		return auth; 
	}
	
	public Auth refreshPlandayAuth() {
		String url = "https://id.planday.com/connect/token"; 		
		
		Auth auth = plandayDAO.getPlandayAuth(); 
		
		HttpClient client = HttpClients.custom().build(); 
		HttpUriRequest request = RequestBuilder.post()
				.setUri(url)
				.addParameter("client_id", auth.getAppId())
				.addParameter("grant_type", "refresh_token")
				.addParameter("refresh_token", auth.getRefreshToken())
				.build(); 
		
		try {
			logger.debug(request.toString());
			HttpResponse response = client.execute(request); 
			HttpEntity entity = response.getEntity();
			
			ObjectMapper mapper = new ObjectMapper(); 		
			
			mapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
			
			AuthJSON jsonObject = mapper.readValue(entity.getContent(), AuthJSON.class); 
			
			auth.setRefreshToken(jsonObject.getRefreshToken());
			auth.setIdToken(jsonObject.getIdToken());
			auth.setAccessToken(jsonObject.getAccessToken());
			auth.setExpires(jsonObject.getExpiresIn());
			auth.setTokenType(jsonObject.getTokenType());
			auth.setScope(jsonObject.getScope());			
						
			plandayDAO.refreshPlandayAuthTokens(auth);
			
			return auth;
		}catch(IOException e) {
			logger.debug("IOException caught in refreshPlandayAuth. " + e.getLocalizedMessage());
		}catch(Exception e) {
			logger.debug("General exception caught in refreshPlandayAuth. " + e.getClass() + ". " +e.getLocalizedMessage());
		}
		return auth;
	}
	
	public PunchClockShiftJSON getAllPunchClockRecords(String from, String to, int offset) throws IOException, ClientProtocolException{
		logger.debug("getAllPunchClockRecords from: " + from + ", to: " + to + ", offset: " + offset);
		
		String url = "https://openapi.planday.com/punchclock/v1.0/punchclockshifts?From=" + from + "&To=" + to; 
		String offsetAddition = "&Offset=" + offset;
		
		Auth auth = getPlandayAuth(); 
		 		
		HttpClient client = HttpClients.custom().build();
		HttpUriRequest request = RequestBuilder.get()
				.setUri(url + offsetAddition)
				.setHeader("X-ClientId", auth.getAppId())
				.setHeader("Authorization", auth.getTokenType() + " " + auth.getAccessToken())
				.build(); 
		
		HttpResponse response = client.execute(request);
		HttpEntity entity = response.getEntity(); 
		
		ObjectMapper mapper = new ObjectMapper();
		
		mapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
		
		punchClockShiftOutputRecursingJSON = mapper.readValue(entity.getContent(), PunchClockShiftJSON.class); 

		logger.debug("Size of the json is " + punchClockShiftOutputRecursingJSON.getData().size() + " before we enter the recursing function");
		
		if(punchClockShiftOutputRecursingJSON.getData().size() >= 50) {
			recurse(auth, PlandayConstants.PUNCHCLOCKSHIFTOUTPUT, url, offset); 
		}
		return punchClockShiftOutputRecursingJSON; 
	}
	
	private boolean recurse(Auth auth, String className, String url, int offset) throws ClientProtocolException, IOException {
		String modifiedUrl = url + "&Offset=" + recursingOffsetCount;
		
		HttpClient client = HttpClients.custom().build();
		HttpUriRequest request = RequestBuilder.get()
				.setUri(modifiedUrl)
				.setHeader("X-ClientId", auth.getAppId())
				.setHeader("Authorization", auth.getTokenType() + " " + auth.getAccessToken())
				.build(); 
		
		logger.debug(request.toString());
		HttpResponse response = client.execute(request);
		HttpEntity entity = response.getEntity(); 
		
		ObjectMapper mapper = new ObjectMapper();
		
		mapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
		
		switch(className) {
		
			case PlandayConstants.PUNCHCLOCKSHIFTOUTPUT:
				
				PunchClockShiftJSON punchClockShiftJSON = mapper.readValue(entity.getContent(), PunchClockShiftJSON.class); 
				
				List<PunchClockShift> oldPunchClockShiftOutputData = punchClockShiftOutputRecursingJSON.getData(); 
				List<PunchClockShift> newPunchClockShiftOutputData = punchClockShiftJSON.getData(); 
				oldPunchClockShiftOutputData.addAll(newPunchClockShiftOutputData); 
				
				punchClockShiftOutputRecursingJSON.setData(oldPunchClockShiftOutputData);
				
				if(punchClockShiftJSON.getData().size() >= 50) {
					recursingOffsetCount = recursingOffsetCount + 50; 
					return recurse(auth, className, url, recursingOffsetCount);
				}
				
				recursingOffsetCount = 50; 
				return true; 
				
			case PlandayConstants.DEPARTMENT:
				
				DepartmentJSON departmentJSON = mapper.readValue(entity.getContent(), DepartmentJSON.class);
				
				List<Department> oldDepartmentData = departmentsRecursingJSON.getData(); 
				List<Department> newDepartmentData = departmentJSON.getData(); 
				oldDepartmentData.addAll(newDepartmentData);
				
				departmentsRecursingJSON.setData(oldDepartmentData);
				
				if(departmentJSON.getData().size() >= 50) {
					recursingOffsetCount = recursingOffsetCount + 50; 
					return recurse(auth, className, url, recursingOffsetCount);
				}
				
				recursingOffsetCount = 50; 
				return true; 
				
			default: 
				logger.debug("Default switch case. We do not know how to handle this.");
				return false; 
		}				
	}
		
	public DepartmentJSON getDepartments() throws ClientProtocolException, IOException {
		String url = "https://openapi.planday.com/hr/v1.0/departments"; 
		Auth auth = getPlandayAuth(); 
		
		HttpClient client = HttpClients.custom().build(); 
		HttpUriRequest request = RequestBuilder.get()
				.setUri(url)
				.setHeader("X-ClientId", auth.getAppId())
				.setHeader("Authorization", auth.getTokenType() + " " + auth.getAccessToken())
				.build(); 
		
		logger.debug(request.toString());
		HttpResponse response = client.execute(request); 
		HttpEntity entity = response.getEntity();
		
		ObjectMapper mapper = new ObjectMapper(); 
		
		mapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
		
		departmentsRecursingJSON = mapper.readValue(entity.getContent(), DepartmentJSON.class); 
		
		logger.debug("Size of the json is " + departmentsRecursingJSON.getData().size() + " before entering the recursing function.");
		
		if(departmentsRecursingJSON.getData().size() >= 50) {
			recursingOffsetCount = recursingOffsetCount + 50;
			recurse(auth, PlandayConstants.DEPARTMENT, url, recursingOffsetCount);
		}
				
		return departmentsRecursingJSON;
	}
		
	public String calculateWorkedTime(List<PunchClockShift> data) throws ParseException {
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
		
		long millis = 0;
		
		for(PunchClockShift shift : data) {
			try {
				Date shiftStart; 
				Date shiftEnd;
				
				if(shift.getStartDateTime() != null && shift.getEndDateTime() != null) {
					
					if(shift.getStartDateTime() != null && shift.getStartDateTime() != "") {
						shiftStart = df.parse(shift.getStartDateTime());
					}else if(shift.getShiftStartDateTime() != null) {
						shiftStart = df.parse(shift.getShiftStartDateTime()); 
					}else {
						logger.debug("We do not have start of punchclocktime or shift itself. Skipping iteration.");
						continue; 
					}
					
					if(shift.getEndDateTime() != null) {
						shiftEnd = df.parse(shift.getEndDateTime()); 
					}else if(shift.getShiftEndDateTime() != null && shift.getShiftEndDateTime() != "") {
						shiftEnd = df.parse(shift.getShiftEndDateTime()); 
					}else {
						logger.debug("We do not have end of punchclocktime or shift itself. Skipping iteration.");
						continue; 
					}
					
					long shiftStartMillis = shiftStart.getTime();  
					long shiftEndMillis = shiftStart.getTime(); 
	
					if(shiftStartMillis > shiftEndMillis) {
						logger.debug("Shift start was greater than shiftend. This might be a problem in planday, or they expect developers to handle that case themselves. Skipping iteration.");
						continue; 
					}
					
					millis = millis + (shiftEndMillis - shiftStartMillis);	
					
					logger.debug("shiftId: " + shift.getShiftId() + ". employeeId: " + shift.getEmployeeId());
					logger.debug("We are adding a shift. getStartDateTime=" + shift.getStartDateTime() + ", getEndDateTime=" + shift.getEndDateTime());
					logger.debug("Actual times was: shiftStartDateTime=" + shift.getShiftStartDateTime() + ", shiftEndDateTime=" + shift.getShiftEndDateTime());
					logger.debug("Shift is approved: " + shift.getIsApproved());
					logger.debug("Adding " + (shiftEnd.getTime() - shiftStart.getTime()) + " to millis");
				}else {
					logger.debug("Shift was missing both start and endtime.. We cant use this.");
				}
				
			// TODO proper error handling once systemDAO contains method to log error data
			}catch(NullPointerException e) {
				logger.debug("Nullpointer caught in calculateWorkedTime for shift with id: " + shift.getShiftId());
			}catch(ParseException e) {
				logger.debug("ParseException caught for shift with id " + shift.getShiftId() + ". Message was " + e.getLocalizedMessage());
			}catch(Exception e) {
				logger.debug(e.getClass() + ", " + e.getLocalizedMessage());
			}
		}
		
		logger.debug("total millis is now " + millis);
		
		String workedTime = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(millis),
	            TimeUnit.MILLISECONDS.toMinutes(millis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(millis)),
	            TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));
		
		return workedTime;
	}

	public List<PunchClockShift> filterPunchclockRecordsByDepartment(int departmentId, PunchClockShiftJSON punchClockShiftsOutput) {
		logger.debug("filterPunchclockRecordsByDepartment for " + departmentId);
		List<PunchClockShift> shiftsToReturn = new ArrayList<PunchClockShift>(); 
		for(PunchClockShift output : punchClockShiftsOutput.getData()) {
			if(output.getDepartmentId() == departmentId) {
				shiftsToReturn.add(output);
			} 
		}
		return shiftsToReturn;
	}

	public EmployeeJSON getEmployeeById(int employeeId) throws ClientProtocolException, IOException {
		String url = "https://openapi.planday.com/hr/v1.0/employees/" + employeeId; 
		
		Auth auth = getPlandayAuth(); 
		 
		HttpClient client = HttpClients.custom().build();
		HttpUriRequest request = RequestBuilder.get()
				.setUri(url)
				.setHeader("X-ClientId", auth.getAppId())
				.setHeader("Authorization", auth.getTokenType() + " " + auth.getAccessToken())
				.build(); 
		
		HttpResponse response = client.execute(request);
		HttpEntity entity = response.getEntity(); 
				
		ObjectMapper mapper = new ObjectMapper();
		
		mapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
		
		EmployeeJSON employee = mapper.readValue(entity.getContent(), EmployeeJSON.class); 
		
		return employee; 
	}
	
	public WorkedHoursAndMissedPunchouts filterMissedPunchoutsList(List<PunchClockShift> output) throws ClientProtocolException, IOException {
		WorkedHoursAndMissedPunchouts workedHoursAndMissedPunchouts = new WorkedHoursAndMissedPunchouts(); 
		
		for(PunchClockShift shift : output) {
			if(shift.getEndDateTime() == null) {
				EmployeeJSON employee = getEmployeeById(shift.getEmployeeId()); 
				
				boolean foundEmployeeInList = false; 
				for(EmployeeMissedPunchouts employeeMissedPunchouts : workedHoursAndMissedPunchouts.getMissedPunchoutsList()) {
					if(employeeMissedPunchouts.getEmployee().getData().getId().equals(shift.getEmployeeId())) {
						logger.debug("Found employee in list. " + employeeMissedPunchouts.getEmployee().getData().getId() + " # " + shift.getEmployeeId());
						foundEmployeeInList = true; 
						employeeMissedPunchouts.getMissedPunchouts().incrementCounterByOne();
						employeeMissedPunchouts.getMissedPunchouts().addDate(shift.getStartDateTime());
					}
				}
				if(foundEmployeeInList != true) {
					logger.debug("Employee was not found in list. Will create a new employeeMissedPunchouts and add to the outer list");
					EmployeeMissedPunchouts employeeMissedPunchouts = new EmployeeMissedPunchouts(); 
					MissedPunchouts missedPunchouts = new MissedPunchouts(); 
					missedPunchouts.incrementCounterByOne();
					missedPunchouts.addDate(shift.getShiftStartDateTime());
					employeeMissedPunchouts.setEmployee(employee);
					employeeMissedPunchouts.setMissedPunchouts(missedPunchouts);
					workedHoursAndMissedPunchouts.add(employeeMissedPunchouts);
				}
			}
		}
		
		return workedHoursAndMissedPunchouts; 
	}
	
	public BreakJSON getPunchclockShiftBreaks (int punchclockShiftId) throws ClientProtocolException, IOException {
		String url = "https://openapi.planday.com/punchclock/v1.0/punchclockshifts/" + punchclockShiftId + "/breaks"; 
		
		Auth auth = getPlandayAuth(); 
		
		HttpClient client = HttpClients.custom().build(); 
		HttpUriRequest request = RequestBuilder.get()
				.setUri(url)
				.setHeader("X-ClientId", auth.getAppId())
				.setHeader("Authorization", auth.getTokenType() + " " + auth.getAccessToken())
				.build(); 
		
		logger.debug(request.toString());
		HttpResponse response = client.execute(request); 
		HttpEntity entity = response.getEntity();
		
		ObjectMapper mapper = new ObjectMapper(); 
		
		mapper.configure(Feature.AUTO_CLOSE_SOURCE, true);
				
		BreakJSON breaks = mapper.readValue(entity.getContent(), BreakJSON.class);

		logger.debug(breaks.getData().toString());
		
		return breaks;
	}
	
	public WorkedHoursPerHour getWorkedHoursbyHour(List<PunchClockShift> shifts) throws ParseException{		
		logger.debug("getWorkedHoursByHour");
		
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
		
		WorkedHoursPerHour workedHoursPerHour = new WorkedHoursPerHour(); 
		Map<Integer, Integer> minutes = new HashMap<Integer, Integer>();  
		for(PunchClockShift shift : shifts) {
			Calendar shiftStart = Calendar.getInstance(); 
			Calendar shiftEnd = Calendar.getInstance(); 
			
			if(shift.getStartDateTime() == null && shift.getStartDateTime() == "" && 
					shift.getEndDateTime() == null && shift.getEndDateTime() == "") {
				logger.debug("Employee missed to clock in and out. We wont add this to the list");
				continue; 
			}
			
			// Do a check for start and end. We already know that both are not missed, but one might be. If thats the case, we use the shifts start and end. 
			if(shift.getStartDateTime() != null && shift.getStartDateTime() != "") {
				shiftStart.setTime(df.parse(shift.getStartDateTime()));					
			}else if(shift.getShiftStartDateTime() != null && shift.getShiftStartDateTime() != "") {
				logger.debug("Shift with id " + shift.getShiftId() + " did not have a punchlock record for when they started. Setting it to the start time of the actual shift.");
				shiftStart.setTime(df.parse(shift.getShiftStartDateTime()));
			}else {
				logger.debug("There was no starting punchclock record and no shift start for shift with id " + shift.getShiftId() + ". Will skip iteration");
				continue;
			}
			
			if(shift.getEndDateTime() != null && shift.getEndDateTime() != "") {
				shiftEnd.setTime(df.parse(shift.getEndDateTime()));
			}else if(shift.getShiftEndDateTime() != null && shift.getShiftEndDateTime() != ""){
				logger.debug("Shift with id " + shift.getShiftId() + " did not have a punchlock record for when they quit. Setting it to the end time of the actual shift.");
				shiftEnd.setTime(df.parse(shift.getShiftEndDateTime()));
			}else {
				logger.debug("There was no ending punchclock record and no shift end for shift with id " + shift.getShiftId() + ". Will skip iteration");
				continue; 
			}
			
			int hourStart = shiftStart.get(Calendar.HOUR_OF_DAY);			
			int hourEnd = shiftEnd.get(Calendar.HOUR_OF_DAY);
			
			for(int i = hourStart; i <= hourEnd; i++) {
				int count = minutes.containsKey(i) ? minutes.get(i) : 0;					
				int newValue;
				
				if(i == hourStart) {
					newValue = count + 60 - shiftStart.get(Calendar.MINUTE); 
				} else if(i == hourEnd) {
					newValue = count + shiftEnd.get(Calendar.MINUTE); 
				} else {
					newValue = count + 60; 
				}				
				minutes.put(i, newValue);				
			}
		}
				
		workedHoursPerHour.setMinutes(minutes);
		
		return workedHoursPerHour; 
	}
	
}
