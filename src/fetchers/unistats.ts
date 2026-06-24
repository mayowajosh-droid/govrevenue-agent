export type UnistatsCourse = {
  institutionName: string;
  courseName: string;
  kisCourseid: string | null;
};

// Unistats was replaced by Discover Uni which has no public JSON API.
// Stub returns [] until a suitable data source is identified.
export async function searchCourses(_query: string): Promise<UnistatsCourse[]> {
  return [];
}
