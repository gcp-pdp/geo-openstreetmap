function euclideanDistances(a, b) {
  var sum = 0;
  var n;
  for (n = 0; n < a.length; n++) {
    sum += Math.pow(a[n] - b[n], 2);
  }
  return Math.sqrt(sum);
}

function cosineSimilarity(a, b) {
  var p = 0;
  var p2 = 0;
  var q2 = 0;
  var n;
  for (var n = 0; n < a.length; n++) {
    p += a[n] * b[n];
    p2 += a[n] * a[n];
    q2 += b[n] * b[n];
  }
  return p / (Math.sqrt(p2) * Math.sqrt(q2));
}
