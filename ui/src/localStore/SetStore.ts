export function SetStore(id: string, data: any) {
  localStorage.setItem(id, data);
}

export function SetNamespaceList(
  nsCookie: string[],
  NS_List_Store: string,
  namespaceVal: string
) {
  let idx = -1;
  for (let i = 0; i < nsCookie.length; i++) {
    if (namespaceVal === nsCookie[i]) {
      idx = i;
      break;
    }
  }
  const arr = nsCookie;
  if (idx !== -1) {
    // moving already present element to front
    const ele = arr[idx];
    arr.splice(idx, 1);
    arr.splice(0, 0, ele);
  } else {
    // appending new element to front
    arr.unshift(namespaceVal);
    if (arr.length > 10) arr.pop();
  }
  localStorage.setItem(NS_List_Store, JSON.stringify(arr));
  return arr.slice();
}
