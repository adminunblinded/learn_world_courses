const express = require('express');
const axios = require('axios');
const redis = require('redis');
const { promisify } = require('util');

const app = express();
const port = process.env.PORT || 3000;

const redisURL = 'redis://default:pi665abmcjJF3GaCjkNFMh2naCkBfCoa@viaduct.proxy.rlwy.net:38050';
const redisClient = redis.createClient(redisURL);
const redisRpushAsync = promisify(redisClient.rpush).bind(redisClient);
const redisLrangeAsync = promisify(redisClient.lrange).bind(redisClient);
const redisQuitAsync = promisify(redisClient.quit).bind(redisClient);

const progressApiUrl = 'https://academy.unblindedmastery.com/admin/api/v2/users/{id}/courses/{cid}/progress';
const userApiUrl = 'https://academy.unblindedmastery.com/admin/api/v2/users/';
const zapierWebhookUrl = 'https://hooks.zapier.com/hooks/catch/15640277/3fgumh1/';

const headers = {
  'Accept': 'application/json',
  'Authorization': 'Bearer scWZswO0q1qJXponQL4mmpwshtyrhdLgng48qD8o',
  'Lw-Client': '5e318802ce0e77a1d77ab772',
};
const requestsPerSecond = 2;
const sleepTime = 1000 / requestsPerSecond;

const salesforceCredentials = {
  client_id: '3MVG9p1Q1BCe9GmBa.vd3k6U6tisbR1DMPjMzaiBN7xn.uqsguNxOYdop1n5P_GB1yHs3gzBQwezqI6q37bh9',
  client_secret: '1AAD66E5E5BF9A0F6FCAA681ED6720A797AC038BC6483379D55C192C1DC93190',
  username: 'admin@unblindedmastery.com',
  password: 'Unblinded2023$',
};


app.post('/receive-access-token', (req, res) => {
  const { accessToken } = req.body;
  if (accessToken) {
    console.log('Received access token:', accessToken);

    // Call the function to fetch and store users with the received access token
    fetchAndStoreUsers(accessToken)
      .then(() => {
        res.sendStatus(200);
      })
      .catch(error => {
        console.error(`Error fetching and storing users: ${error.message}`);
        res.status(500).json({ error: 'Internal server error' });
      });
  } else {
    res.status(400).json({ error: 'Access token not received' });
  }
});

// Trigger the Zapier webhook to fetch the access token
async function triggerZapierWebhook() {
  try {
    await axios.post(zapierWebhookUrl);
    console.log('Zapier webhook triggered successfully.');
  } catch (error) {
    console.error(`Error triggering Zapier webhook: ${error.message}`);
  }
}

triggerZapierWebhook();

async function getAccountId(email, accessToken) {
  try {
    const queryUrl = `https://unblindedmastery.my.salesforce.com/services/data/v58.0/query/?q=SELECT+Id+FROM+Account+WHERE+Email__c='${email}'`;
    const response = await axios.get(queryUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
    });

    if (response.data.records.length > 0) {
      return response.data.records[0].Id;
    } else {
      throw new Error('Account not found for the provided email');
    }
  } catch (error) {
    throw new Error(`Error retrieving AccountId: ${error.response ? error.response.data : error.message}`);
  }
}

async function getCourseId(courseTitle, accessToken) {
  try {
    const queryUrl = `https://unblindedmastery.my.salesforce.com/services/data/v58.0/query/?q=SELECT+Id+FROM+Course__c+WHERE+Name='${courseTitle}'`;
    const response = await axios.get(queryUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
    });

    if (response.data.records.length > 0) {
      return response.data.records[0].Id;
    } else {
      throw new Error('Course not found for the provided title');
    }
  } catch (error) {
    throw new Error(`Error retrieving CourseId: ${error.response ? error.response.data : error.message}`);
  }
}

async function createSalesforceRecord(email, courseTitle, timeOnCourse) {
  const url = 'https://login.salesforce.com/services/oauth2/token';
  const params = new URLSearchParams({
    grant_type: 'password',
    client_id: salesforceCredentials.client_id,
    client_secret: salesforceCredentials.client_secret,
    username: salesforceCredentials.username,
    password: salesforceCredentials.password,
  });

  try {
    // Get Salesforce access token
    const response = await axios.post(url, params);
    const accessToken = response.data.access_token;

    // Get AccountId for the provided email
    const accountId = await getAccountId(email, accessToken);

    // Get CourseId for the provided courseTitle
    const courseId = await getCourseId(courseTitle, accessToken);

    // Check if a record with the same AccountId and CourseId exists
    const queryUrl = `https://unblindedmastery.my.salesforce.com/services/data/v58.0/query/?q=SELECT Id, Course_Time__c FROM Course_Association__c WHERE Account__c = '${accountId}' AND Course__c = '${courseId}'`;
    const queryResponse = await axios.get(queryUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
    });

    if (queryResponse.data.records.length > 0) {
      // Record exists, compare Course_Time__c
      const existingRecord = queryResponse.data.records[0];
      if (existingRecord.Course_Time__c != timeOnCourse.toFixed(2)) {
        // Update the existing record
        const updateRecordUrl = `https://unblindedmastery.my.salesforce.com/services/data/v58.0/sobjects/Course_Association__c/${existingRecord.Id}`;
        const updateRecordData = {
          Course_Time__c: timeOnCourse.toFixed(2),
        };
        await axios.patch(updateRecordUrl, updateRecordData, {
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
          },
        });
        console.log('Record updated successfully:', existingRecord.Id);
      } else {
        // No need to update, Course_Time__c is the same
        console.log('Record already up-to-date, no changes needed.');
      }
    } else {
      // Record doesn't exist, create a new one
      const createRecordUrl = 'https://unblindedmastery.my.salesforce.com/services/data/v58.0/sobjects/Course_Association__c/';
      const recordData = {
        attributes: {
          type: 'Course_Association__c',
        },
        Account__c: accountId,
        Course__c: courseId,
        Course_Time__c: timeOnCourse.toFixed(2),
      };
      const createRecordResponse = await axios.post(createRecordUrl, recordData, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
      });

      console.log('Record created successfully:', createRecordResponse.data);
    }
  } catch (error) {
    console.error('Error creating/updating record:', error.response ? error.response.data : error.message);
  }
}

async function getAllUsers(accessToken, pageNumber) {
  const url = `https://academy.unblindedmastery.com/admin/api/v2/users?page=${pageNumber}`;
  const headers = {
    'Authorization': `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
    'Lw-Client': '5e318802ce0e77a1d77ab772',
  };

  try {
    const response = await axios.get(url, { headers });
    return response.data.data || [];
  } catch (error) {
    console.error(`Error fetching user data: ${error.message}`);
    return [];
  }
}

async function fetchBatchUsers(accessToken, startPage, endPage) {
  const batchUsers = [];

  for (let pageNumber = startPage; pageNumber <= endPage; pageNumber++) {
    const usersData = await getAllUsers(accessToken, pageNumber);

    for (const user of usersData) {
      const userInfo = { email: user.email };
      batchUsers.push(userInfo);
    }
  }

  return batchUsers;
}

async function fetchAndStoreUsers(accessToken){
  const totalBatches = 1; // 180 requests / 2 requests per second
  const requestsPerBatch = 2;
  const delayBetweenRequests = 1000 / requestsPerBatch;

  const existingUserEmails = await getUserEmailsFromRedis(); // Fetch existing user emails from Redis
  const allUsers = [];

  for (let batchNumber = 1; batchNumber <= totalBatches; batchNumber++) {
    const startPage = (batchNumber - 1) * requestsPerBatch + 1;
    const endPage = batchNumber * requestsPerBatch;

    const batchUsers = await fetchBatchUsers(accessToken, startPage, endPage);
    allUsers.push(...batchUsers);

    if (batchNumber < totalBatches) {
      await new Promise(resolve => setTimeout(resolve, delayBetweenRequests));
    }
  }

  for (const user of allUsers) {
    const userEmail = user.email;
    // Check if the user email already exists in Redis
    if (!existingUserEmails.includes(userEmail)) {
      await redisRpushAsync('user_emails', userEmail);
      existingUserEmails.push(userEmail); // Update the existing user emails list
    }
  }

  res.json({ message: 'User information written to Redis.' });

  const userCourseProgress = {};

  async function getUserEmailsFromRedis() {
    try {
      const user_emails = await redisLrangeAsync('user_emails', 0, -1);
      return user_emails;
    } catch (error) {
      console.error(`Error fetching user emails from Redis. Error: ${error.message}`);
      throw error;
    }
  }

  async function fetchCourses(userEmail) {
    const url = `${userApiUrl}${userEmail}/courses`;
    try {
      const response = await axios.get(url, { headers });
      const data = response.data.data || [];
      const promises = data.map(async (course) => {
        const courseId = course.course.id;
        const courseTitle = course.course.title;
        await fetchAndStoreProgress(userEmail, courseId, courseTitle);
      });
      await Promise.all(promises);
    } catch (error) {
      console.error(`Failed to fetch courses for user ${userEmail}. Error: ${error.message}`);
    }
  }

  async function fetchAndStoreProgress(userEmail, courseId, courseTitle) {
    const progressUrl = progressApiUrl.replace('{id}', userEmail).replace('{cid}', courseId);
    try {
      const response = await axios.get(progressUrl, { headers });
      if (response.status === 200) {
        const progressData = response.data;
        const key = `${userEmail}:${courseId}`;
        if (!userCourseProgress[key]) {
          userCourseProgress[key] = { time_on_course: progressData.time_on_course / 60.0, course_title: courseTitle };
        } else {
          userCourseProgress[key].time_on_course = progressData.time_on_course / 60.0;
        }
      } else {
        console.error(`Failed to fetch progress for user ${userEmail}, course ${courseId}. Status code: ${response.status}`);
      }
    } catch (error) {
      console.error(`Error fetching progress for user ${userEmail}, course ${courseId}. Error: ${error.message}`);
    }
  }

  try {
    const user_emails = await getUserEmailsFromRedis();
    for (const userEmail of user_emails) {
      await fetchCourses(userEmail);
      await new Promise(resolve => setTimeout(resolve, sleepTime));
    }

    console.log('\nUser Course Progress:');
    for (const [key, value] of Object.entries(userCourseProgress)) {
      const [userEmail, courseId] = key.split(':');
      const { time_on_course, course_title } = value;

      await createSalesforceRecord(userEmail, course_title, time_on_course);
    }
  } finally {
    await redisQuitAsync();
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
