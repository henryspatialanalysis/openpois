/**
 * Creates a debounced query executor that cancels pending queries
 * when a new one is requested.
 */
export function createQueryDebouncer(delayMs = 300) {
  let timer = null
  let currentAbort = null

  /**
   * Schedule a query function to run after the debounce delay.
   * Cancels any pending scheduled query.
   *
   * @param {Function} queryFn - Async function to execute
   * @returns {Promise} Resolves with query result or rejects if cancelled
   */
  function schedule(queryFn) {
    // Cancel pending timer
    if (timer) clearTimeout(timer)

    // Signal abort to any in-flight query
    if (currentAbort) currentAbort.abort()

    const abortController = new AbortController()
    currentAbort = abortController

    return new Promise((resolve, reject) => {
      timer = setTimeout(async () => {
        if (abortController.signal.aborted) {
          reject(new DOMException('Aborted', 'AbortError'))
          return
        }
        try {
          const result = await queryFn(abortController.signal)
          if (!abortController.signal.aborted) {
            resolve(result)
          } else {
            reject(new DOMException('Aborted', 'AbortError'))
          }
        } catch (err) {
          reject(err)
        }
      }, delayMs)
    })
  }

  function cancel() {
    if (timer) clearTimeout(timer)
    if (currentAbort) currentAbort.abort()
  }

  return { schedule, cancel }
}
