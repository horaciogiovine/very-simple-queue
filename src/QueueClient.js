/**
 * @class QueueClient
 */
class QueueClient {
  /**
   * @type {module:types.Driver}
   * @private
   */
  #dbDriver;

  /**
   * @type {module:types.UuidGenerator}
   * @private
   */
  #uuidGenerator;

  /**
   * @type {module:helpers.getCurrentTimestamp}
   * @private
   */
  #getCurrentTimestamp;

  /**
   * @type {Worker}
   * @private
   */
  #worker;

  /**
   * @type {Function}
   * @private
   */
  #handleJob;

  /**
   * @param {module:types.Driver} dbDriver - The driver for the database.
   * @param {module:types.UuidGenerator} uuidGenerator - The UUID generator.
   * @param {module:helpers.getCurrentTimestamp} getCurrentTimestamp - The function to get the current timestamp.
   * @param {Object} worker - The worker object.
   */
  constructor(dbDriver, uuidGenerator, getCurrentTimestamp, worker) {
    this.#dbDriver = dbDriver;
    this.#uuidGenerator = uuidGenerator;
    this.#getCurrentTimestamp = getCurrentTimestamp;
    this.#worker = worker;

    // console.log('--setting should process');
    this.shouldProcess = true;

    /**
     * Handles a job and executes the provided job handler function.
     *
     * @param {module:types.Job} job - The job to handle.
     * @param {module:types.JobHandler} jobHandler - The handler function for the job.
     * @param {boolean} [throwErrorOnFailure=false] - If a job fails, mark it as failed and throw an error.
     * @returns {Promise<null|*>} - The result of the job handler function.
     * @throws Error
     * @private
     */
    this.#handleJob = async (job, jobHandler, throwErrorOnFailure = false) => {
      if (!job) {
        return null;
      }

      try {
        // console.log('---------before jobHandler');
        const result = await jobHandler(job.payload);

        // console.log('--- handleJob: ', job);
        job.completed_at = this.#getCurrentTimestamp();
        // job.reserved_at
        // job.cache_time
        // job.cached_at
        job.is_cached = true;
        job.http_status = result.status;

        // console.log('--#handleJob result: ', result);
        await Promise.all([
          this.#dbDriver.storeFinishedJob(job),
          this.#dbDriver.deleteJob(job.uuid)
        ]);

        return result;
      } catch (error) {
        error = this.parseAxiosError(error);

        const http_status = error.response ? error.response.status : 500;
        await this.#dbDriver.markJobAsFailed(job.uuid, http_status);

        if (throwErrorOnFailure) {
          throw new Error(`Job with uuid ${job.uuid} failed`);
        }
      }

      return null;
    };
  }

  parseAxiosError(error) {
    if (error.response) {
      // The request was made and the server responded with a status code
      // that falls out of the range of 2xx
      // console.log(error.response.data);
      console.log(error.response.status);
      // console.log(error.response.headers);
      return error;
    } else if (error.request) {
      // The request was made but no response was received
      // `error.request` is an instance of XMLHttpRequest in the browser and an instance of
      // http.ClientRequest in node.js
      console.log(error.request);
      return error;
    } else {
      // Something happened in setting up the request that triggered an Error
      console.log('Error', error.message);
      return error;
    }
  };

  /**
   * Creates the necessary database structure for jobs.
   *
   * @returns {Promise<void>}
   */
  async createJobsDbStructure() {
    await this.#dbDriver.createJobsDbStructure();
  }

  extractDomain(url) {
    let domain;

    // Find & remove protocol (http, ftp, etc.) and get domain
    if (url.indexOf("://") > -1) {
      domain = url.split('/')[2];
    }
    else {
      domain = url.split('/')[0];
    }

    // Find & remove port number
    domain = domain.split(':')[0];

    // Find & remove query string
    domain = domain.split('?')[0];

    return domain;
  }

  /**
   * Pushes a job to the queue.
   *
   * @param {Object} payload - The payload of the job.
   * @param {string|null} [queue='default'] - The queue to push the job to.
   * @returns {Promise<string>} - The UUID of the created job.
   */
  async pushJob(payload, queue = 'default') {

    // console.log('--pushjob: domain: ', payload);
    const job = {
      uuid: this.#uuidGenerator(),
      queue,
      payload,
      created_at: this.#getCurrentTimestamp(),
      reserved_at: null,
      failed_at: null,
      domain: this.extractDomain(payload.url),
      cache_time: null,
      cached_at: null,
      is_cached: false,
      http_status: null
    };

    let existingJob = [await this.#dbDriver.getJobByPayload(payload)].flat();

    if (!existingJob.length) {
      // console.log('-- new job to insert: ', job);
      await this.#dbDriver.storeJob(job);
    } else {
      // Effectively moving a job to the top of the queue
      const previousJob = existingJob[0];
      previousJob.created_at = job.created_at;

      await this.#dbDriver.updateJobByUuid(previousJob);
    }

    return job.uuid;
  }

  /**
   * Handles the next available job in the specified queue.
   *
   * @param {module:types.JobHandler} jobHandler - The handler function for the job.
   * @param {string} [queue='default'] - The queue to handle the job from.
   * @param {boolean} [throwErrorOnFailure=false] - If a job fails, mark it as failed and throw an error.
   * @returns {Promise<*>} - The result of the job handler function.
   */
  async handleJob(jobHandler, queue = 'default', throwErrorOnFailure = false) {
    const job = await this.#dbDriver.getJob(queue);

    return this.#handleJob(job, jobHandler, throwErrorOnFailure);
  }

  /**
   * Handles a job specified by its UUID.
   *
   * @param {module:types.JobHandler} jobHandler - The handler function for the job.
   * @param {string} jobUuid - The UUID of the job.
   * @param {boolean} [throwErrorOnFailure=false] - If a job fails, mark it as failed and throw an error.
   * @returns {Promise<*>} - The result of the job handler function.
   */
  async handleJobByUuid(jobHandler, jobUuid, throwErrorOnFailure = false) {
    const job = await this.#dbDriver.getJobByUuid(jobUuid);

    return this.#handleJob(job, jobHandler, throwErrorOnFailure);
  }

  /**
   * Enqueues a job specified by its UUID.
   *
   * @param {string} jobUuid - The UUID of the job.
   * @returns {Promise<*>}
   */
  async enqueueJobByUuid(jobUuid) {
    const job = await this.#dbDriver.getJobByUuid(jobUuid);
    // console.log('--- enqueueJobByUuid: ', job);
    job.failed_at = null;
    job.reserved_at = null;
    job.created_at = this.#getCurrentTimestamp();
    job.cache_time = null;
    job.cached_at = null;
    job.is_cached = false;
    job.http_status = null;



    return await this.#dbDriver.updateJobByUuid(job);
  }

  /**
   * Handles a finished job specified by its UUID.
   *
   * @param {string} jobUuid - The UUID of the finished job.
   * @returns {Promise<*>}
   */
  async handleFinishedJobByUuid(jobUuid) {
    // console.log('--: ', { jobUuid });
    const job = await this.#dbDriver.getFinishedJobByUuid(jobUuid);

    // console.log('-- handleFinishedJobByUuid: ', job);

    return this.pushJob(JSON.parse(job.payload), job.queue);
  }

  /**
   * Handles a failed job in the specified queue.
   *
   * @param {module:types.JobHandler} jobHandler - The handler function for the job.
   * @param {string} [queue='default'] - The queue to handle the job from.
   * @param {boolean} [throwErrorOnFailure=false] - If a job fails, mark it as failed and throw an error.
   * @returns {Promise<*>} - The result of the job handler function.
   */
  async handleFailedJob(jobHandler, queue = 'default', throwErrorOnFailure = false) {
    const job = await this.#dbDriver.getFailedJob(queue);

    return this.#handleJob(job, jobHandler, throwErrorOnFailure);
  }

  /**
   * Closes the database connection.
   *
   * @returns {Promise<void>}
   */
  async closeConnection() {
    await this.#dbDriver.closeConnection();
  }

  /**
   * Starts the worker to process jobs.
   *
   * @param {module:types.JobHandler} jobHandler - The handler function for the job.
   * @param {module:types.WorkerSettings} settings - The settings for the worker.
   * @returns {Promise<void>}
   */
  async work(jobHandler, settings) {
    // console.log('-- queue client call to work');
    await this.#worker.work(this, jobHandler, settings);
  }

  /**
   * Signals the workers to stop working after they have finished with the current job.
   *
   * @returns {void}
   */
  shutdown() {
    this.disableProcessing();
  }

  /**
   * Checks if the workers should be shut down.
   *
   * @returns {boolean} - True if workers should be shut down, false otherwise.
   */
  shouldShutdown() {
    return this.getProcessingStatus();
  }

  /************** Custom Methods **************/

  /**
   * Retrieves all jobs in the specified queue.
   *
   * @param {string} [queue='default'] - The queue to retrieve jobs from.
   * @returns {Promise<Array>} - Array of jobs.
   */
  async getAllJobsByQueue(queue = 'default') {
    const jobs = await this.#dbDriver.getAllJobsByQueue(queue);

    return jobs;
  }

  /**
   * Retrieves all finished jobs in the specified queue.
   *
   * @param {string} [queue='default'] - The queue to retrieve finished jobs from.
   * @returns {Promise<Array>} - Array of finished jobs.
   */
  async getFinishedJobsByQueue(queue = 'default') {
    const jobs = await this.#dbDriver.getFinishedJobsByQueue(queue);

    return jobs;
  }

  /**
   * Enqueues all reserved jobs in the specified queue.
   *
   * @param {string} [queue='default'] - The queue to enqueue reserved jobs from.
   * @returns {Promise<void>}
   */
  async enqueueAllReservedJobs(queue = 'default') {
    return await this.#dbDriver.enqueueAllReservedJobs(queue);
  }

  /**
   * Turns on the worker.
   *
   * @returns {void}
   */
  turnOn() {
    // console.log('-- turning on');
    this.enableProcessing();
  }

  disableProcessing() {
    // console.log('--disableProcessing');
    this.shouldProcess = false;
  }

  enableProcessing() {
    // console.log('--enableProcessing');
    this.shouldProcess = true;
  }

  getProcessingStatus() {
    // console.trace();
    // console.log('--getProcessingStatus: ', this.shouldProcess);
    return this.shouldProcess;
  }
}

module.exports = QueueClient;
